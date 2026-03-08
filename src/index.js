import { createReadStream } from 'fs';
import { readFile, writeFile } from 'fs/promises';
import { createInterface } from 'readline';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DESCRIPTION_DIR = path.join(__dirname, '..', 'description');

// ---------------------------------------------------------------------------
// CPU parsing — normalizes all formats to millicores
// "500m" -> 500,  "2" -> 2000,  "1.316" -> 1316
// ---------------------------------------------------------------------------
function parseCpuToMillicores(raw) {
  if (typeof raw === 'string' && raw.endsWith('m')) {
    return parseFloat(raw.slice(0, -1));
  }
  return parseFloat(raw) * 1000;
}

// ---------------------------------------------------------------------------
// Policy loader — reads policies.json and indexes by "namespace:deployment"
// ---------------------------------------------------------------------------
async function loadPolicies(filePath) {
  const fileContent = await readFile(filePath, 'utf-8');
  const rawPolicies = JSON.parse(fileContent);

  const policyByDeployment = new Map();

  for (const raw of rawPolicies) {
    const deploymentKey = `${raw.scaleTargetRef.namespace}:${raw.scaleTargetRef.name}`;

    policyByDeployment.set(deploymentKey, {
      namespace: raw.scaleTargetRef.namespace,
      deployment: raw.scaleTargetRef.name,
      cpuThresholdPercent: parseFloat(raw.triggers[0].metadata.value),
      scaleUpWindowSeconds: raw.behavior.scaleUp.stabilizationWindowSeconds,
      scaleDownWindowSeconds: raw.behavior.scaleDown.stabilizationWindowSeconds,
      cooldownSeconds: raw.cooldownPeriod,
    });
  }

  return policyByDeployment;
}

// ---------------------------------------------------------------------------
// Deployment state — one per namespace:deployment
// ---------------------------------------------------------------------------
function createDeploymentState() {
  return {
    podMetrics: new Map(),         // podName -> { cpuUsage, cpuLimit }
    lastAvgCpuPercent: null,
    lastAvgTimestamp: null,

    scaleUpDetectedAt: null,       // when avg first crossed above threshold
    scaleDownDetectedAt: null,     // when avg first crossed below threshold
    cooldownUntil: null,           // actions blocked until this ISO timestamp
  };
}

// ---------------------------------------------------------------------------
// Compute average CPU utilization % across all known pods in a deployment
// ---------------------------------------------------------------------------
function computeAvgCpuPercent(deploymentState) {
  const pods = deploymentState.podMetrics;
  if (pods.size === 0) return null;

  let totalUtilization = 0;
  for (const [, pod] of pods) {
    totalUtilization += (pod.cpuUsage / pod.cpuLimit) * 100;
  }
  return totalUtilization / pods.size;
}

// ---------------------------------------------------------------------------
// Update the detection flags based on where the avg CPU sits vs threshold.
// This runs every timestamp, even during cooldown, so the "clock" is always
// accurate for when the condition first appeared.
// ---------------------------------------------------------------------------
function updateDetectionFlags(deploymentState, avgCpuPercent, threshold, timestamp) {
  const aboveThreshold = avgCpuPercent > threshold;
  const belowThreshold = avgCpuPercent < threshold;

  if (aboveThreshold) {
    if (deploymentState.scaleUpDetectedAt === null) {
      deploymentState.scaleUpDetectedAt = timestamp;
    }
    deploymentState.scaleDownDetectedAt = null;
  } else if (belowThreshold) {
    if (deploymentState.scaleDownDetectedAt === null) {
      deploymentState.scaleDownDetectedAt = timestamp;
    }
    deploymentState.scaleUpDetectedAt = null;
  } else {
    deploymentState.scaleUpDetectedAt = null;
    deploymentState.scaleDownDetectedAt = null;
  }
}

// ---------------------------------------------------------------------------
// Check if cooldown has expired. Returns true if still in cooldown.
// ---------------------------------------------------------------------------
function isCooldownActive(deploymentState, currentTimeMs) {
  if (deploymentState.cooldownUntil === null) return false;

  if (currentTimeMs < new Date(deploymentState.cooldownUntil).getTime()) {
    return true;
  }

  deploymentState.cooldownUntil = null;
  return false;
}

// ---------------------------------------------------------------------------
// Try to emit a scaling action if the stabilization window has elapsed.
// Returns the action object if emitted, or null.
// ---------------------------------------------------------------------------
function tryEmitScalingAction(deploymentState, policy, avgCpuPercent, currentTimestamp, currentTimeMs) {
  const rounded = Math.round(avgCpuPercent * 10) / 10;

  if (deploymentState.scaleUpDetectedAt !== null) {
    const elapsedMs = currentTimeMs - new Date(deploymentState.scaleUpDetectedAt).getTime();

    if (elapsedMs >= policy.scaleUpWindowSeconds * 1000) {
      deploymentState.scaleUpDetectedAt = null;
      deploymentState.cooldownUntil = new Date(currentTimeMs + policy.cooldownSeconds * 1000).toISOString();

      return {
        timestamp: currentTimestamp,
        namespace: policy.namespace,
        deployment: policy.deployment,
        action: 'scaleUp',
        averageCPU: rounded,
        reason: `CPU > ${policy.cpuThresholdPercent}% for ${policy.scaleUpWindowSeconds} seconds`,
      };
    }
  }

  if (deploymentState.scaleDownDetectedAt !== null) {
    const elapsedMs = currentTimeMs - new Date(deploymentState.scaleDownDetectedAt).getTime();

    if (elapsedMs >= policy.scaleDownWindowSeconds * 1000) {
      deploymentState.scaleDownDetectedAt = null;
      deploymentState.cooldownUntil = new Date(currentTimeMs + policy.cooldownSeconds * 1000).toISOString();

      return {
        timestamp: currentTimestamp,
        namespace: policy.namespace,
        deployment: policy.deployment,
        action: 'scaleDown',
        averageCPU: rounded,
        reason: `CPU < ${policy.cpuThresholdPercent}% for ${policy.scaleDownWindowSeconds} seconds`,
      };
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Evaluate all deployments at a completed timestamp boundary
// ---------------------------------------------------------------------------
function evaluateAllDeployments(timestamp, deploymentStates, policyByDeployment, scalingDecisions) {
  const currentTimeMs = new Date(timestamp).getTime();

  for (const [deploymentKey, deploymentState] of deploymentStates) {
    const policy = policyByDeployment.get(deploymentKey);
    if (!policy) continue;

    const avgCpuPercent = computeAvgCpuPercent(deploymentState);
    if (avgCpuPercent === null) continue;

    deploymentState.lastAvgCpuPercent = avgCpuPercent;
    deploymentState.lastAvgTimestamp = timestamp;

    updateDetectionFlags(deploymentState, avgCpuPercent, policy.cpuThresholdPercent, timestamp);

    if (isCooldownActive(deploymentState, currentTimeMs)) continue;

    const action = tryEmitScalingAction(deploymentState, policy, avgCpuPercent, timestamp, currentTimeMs);
    if (action) {
      scalingDecisions.push(action);
    }
  }
}

// ---------------------------------------------------------------------------
// Store a pod metric event into the deployment state
// ---------------------------------------------------------------------------
function recordPodMetric(deploymentStates, event) {
  const deploymentKey = `${event.namespace}:${event.deployment}`;

  let deploymentState = deploymentStates.get(deploymentKey);
  if (!deploymentState) {
    deploymentState = createDeploymentState();
    deploymentStates.set(deploymentKey, deploymentState);
  }

  deploymentState.podMetrics.set(event.pod, {
    cpuUsage: parseCpuToMillicores(event.cpuUsage),
    cpuLimit: parseCpuToMillicores(event.cpuLimit),
  });
}

// ---------------------------------------------------------------------------
// Stream events line-by-line, processing each completed timestamp batch
// ---------------------------------------------------------------------------
async function processEventStream(eventsPath, deploymentStates, policyByDeployment) {
  const scalingDecisions = [];
  let lastTimestamp = null;

  const lineReader = createInterface({
    input: createReadStream(eventsPath, { encoding: 'utf-8' }),
    crlfDelay: Infinity,
  });

  for await (const line of lineReader) {
    if (!line.trim()) continue;

    const event = JSON.parse(line);

    // Timestamp boundary — evaluate all deployments for the previous timestamp
    if (lastTimestamp !== null && event.timestamp !== lastTimestamp) {
      evaluateAllDeployments(lastTimestamp, deploymentStates, policyByDeployment, scalingDecisions);
    }
    lastTimestamp = event.timestamp;

    recordPodMetric(deploymentStates, event);
  }

  // Flush the final timestamp
  if (lastTimestamp !== null) {
    evaluateAllDeployments(lastTimestamp, deploymentStates, policyByDeployment, scalingDecisions);
  }

  return scalingDecisions;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const policiesPath = path.join(DESCRIPTION_DIR, 'policies.json');
  const eventsPath = path.join(DESCRIPTION_DIR, 'events.log');
  const outputPath = path.join(DESCRIPTION_DIR, 'output.json');

  const policyByDeployment = await loadPolicies(policiesPath);
  console.log(`Loaded ${policyByDeployment.size} policies`);

  const deploymentStates = new Map();
  for (const deploymentKey of policyByDeployment.keys()) {
    deploymentStates.set(deploymentKey, createDeploymentState());
  }

  const scalingDecisions = await processEventStream(eventsPath, deploymentStates, policyByDeployment);

  await writeFile(outputPath, JSON.stringify(scalingDecisions, null, 2), 'utf-8');
  console.log(`Done. ${scalingDecisions.length} scaling decisions written to ${outputPath}`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
