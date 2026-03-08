import { createReadStream } from 'fs';
import { readFile, writeFile } from 'fs/promises';
import { createInterface } from 'readline';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DESCRIPTION_DIR = path.join(__dirname, '..', 'description');

// ---------------------------------------------------------------------------
// CPU parsing: "500m" -> 500, "2" -> 2000, "1.316" -> 1316
// ---------------------------------------------------------------------------
function parseCpu(raw) {
  if (typeof raw === 'string' && raw.endsWith('m')) {
    return parseFloat(raw.slice(0, -1));
  }
  return parseFloat(raw) * 1000;
}

// ---------------------------------------------------------------------------
// Policy loader
// ---------------------------------------------------------------------------
async function loadPolicies(filePath) {
  const raw = await readFile(filePath, 'utf-8');
  const policies = JSON.parse(raw);
  const policyMap = new Map();
  for (const p of policies) {
    const key = `${p.scaleTargetRef.namespace}:${p.scaleTargetRef.name}`;
    policyMap.set(key, {
      namespace: p.scaleTargetRef.namespace,
      deployment: p.scaleTargetRef.name,
      threshold: parseFloat(p.triggers[0].metadata.value),
      scaleUpWindow: p.behavior.scaleUp.stabilizationWindowSeconds,
      scaleDownWindow: p.behavior.scaleDown.stabilizationWindowSeconds,
      cooldownPeriod: p.cooldownPeriod,
    });
  }
  return policyMap;
}

// ---------------------------------------------------------------------------
// Deployment state — one per namespace:deployment
// ---------------------------------------------------------------------------
function createDeploymentState() {
  return {
    pods: new Map(),              // podName -> { cpuUsage, cpuLimit, timestamp }
    lastAvgCpu: null,             // last computed average CPU utilization %
    lastAvgTimestamp: null,       // timestamp of last computed average

    scaleUpDetectedAt: null,      // timestamp when we first detected avg > threshold
    scaleDownDetectedAt: null,    // timestamp when we first detected avg < threshold
    cooldownUntil: null,          // no actions allowed until this timestamp
  };
}

// ---------------------------------------------------------------------------
// Core: process a completed timestamp for all deployments that had events
// ---------------------------------------------------------------------------
function processTimestamp(timestamp, deploymentStates, policyMap, scalingDecisions) {
  const tsMs = new Date(timestamp).getTime();

  for (const [key, state] of deploymentStates) {
    const policy = policyMap.get(key);
    if (!policy) continue;

    // Compute average CPU utilization across all known pods
    let totalUtil = 0;
    let podCount = 0;
    for (const [, pod] of state.pods) {
      const util = (pod.cpuUsage / pod.cpuLimit) * 100;
      totalUtil += util;
      podCount++;
    }
    if (podCount === 0) continue;

    const avgCpu = totalUtil / podCount;
    state.lastAvgCpu = avgCpu;
    state.lastAvgTimestamp = timestamp;

    const aboveThreshold = avgCpu > policy.threshold;
    const belowThreshold = avgCpu < policy.threshold;

    // Update detection timestamps
    if (aboveThreshold) {
      if (state.scaleUpDetectedAt === null) {
        state.scaleUpDetectedAt = timestamp;
      }
      state.scaleDownDetectedAt = null;
    } else if (belowThreshold) {
      if (state.scaleDownDetectedAt === null) {
        state.scaleDownDetectedAt = timestamp;
      }
      state.scaleUpDetectedAt = null;
    } else {
      // exactly at threshold — reset both
      state.scaleUpDetectedAt = null;
      state.scaleDownDetectedAt = null;
    }

    // Check cooldown
    if (state.cooldownUntil !== null && tsMs < new Date(state.cooldownUntil).getTime()) {
      continue;
    }
    // If cooldown just expired, clear it
    if (state.cooldownUntil !== null && tsMs >= new Date(state.cooldownUntil).getTime()) {
      state.cooldownUntil = null;
    }

    // Check scale-up stabilization window
    if (state.scaleUpDetectedAt !== null) {
      const detectedMs = new Date(state.scaleUpDetectedAt).getTime();
      if (tsMs - detectedMs >= policy.scaleUpWindow * 1000) {
        scalingDecisions.push({
          timestamp,
          namespace: policy.namespace,
          deployment: policy.deployment,
          action: 'scaleUp',
          averageCPU: Math.round(avgCpu * 10) / 10,
          reason: `CPU > ${policy.threshold}% for ${policy.scaleUpWindow} seconds`,
        });
        state.scaleUpDetectedAt = null;
        state.cooldownUntil = new Date(tsMs + policy.cooldownPeriod * 1000).toISOString();
      }
    }

    // Check scale-down stabilization window
    if (state.scaleDownDetectedAt !== null) {
      const detectedMs = new Date(state.scaleDownDetectedAt).getTime();
      if (tsMs - detectedMs >= policy.scaleDownWindow * 1000) {
        scalingDecisions.push({
          timestamp,
          namespace: policy.namespace,
          deployment: policy.deployment,
          action: 'scaleDown',
          averageCPU: Math.round(avgCpu * 10) / 10,
          reason: `CPU < ${policy.threshold}% for ${policy.scaleDownWindow} seconds`,
        });
        state.scaleDownDetectedAt = null;
        state.cooldownUntil = new Date(tsMs + policy.cooldownPeriod * 1000).toISOString();
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const policiesPath = path.join(DESCRIPTION_DIR, 'policies.json');
  const eventsPath = path.join(DESCRIPTION_DIR, 'events.log');
  const outputPath = path.join(DESCRIPTION_DIR, 'output.json');

  const policyMap = await loadPolicies(policiesPath);
  console.log(`Loaded ${policyMap.size} policies`);

  const deploymentStates = new Map();
  // Initialize state for each policy
  for (const key of policyMap.keys()) {
    deploymentStates.set(key, createDeploymentState());
  }

  const scalingDecisions = [];
  let lastTimestamp = null;

  const rl = createInterface({
    input: createReadStream(eventsPath, { encoding: 'utf-8' }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if (!line.trim()) continue;

    const event = JSON.parse(line);
    const key = `${event.namespace}:${event.deployment}`;

    // When timestamp changes, process the previous timestamp batch
    if (lastTimestamp !== null && event.timestamp !== lastTimestamp) {
      processTimestamp(lastTimestamp, deploymentStates, policyMap, scalingDecisions);
    }
    lastTimestamp = event.timestamp;

    // Update pod CPU data
    let state = deploymentStates.get(key);
    if (!state) {
      state = createDeploymentState();
      deploymentStates.set(key, state);
    }
    state.pods.set(event.pod, {
      cpuUsage: parseCpu(event.cpuUsage),
      cpuLimit: parseCpu(event.cpuLimit),
      timestamp: event.timestamp,
    });
  }

  // Process the final timestamp
  if (lastTimestamp !== null) {
    processTimestamp(lastTimestamp, deploymentStates, policyMap, scalingDecisions);
  }

  await writeFile(outputPath, JSON.stringify(scalingDecisions, null, 2), 'utf-8');
  console.log(`Done. ${scalingDecisions.length} scaling decisions written to ${outputPath}`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
