import fetch from 'node-fetch';

const ORCHESTRATOR_URL = process.env.ORCH_URL || 'http://localhost:4000';
const POLL_INTERVAL = 2000; // 2 seconds

// Default tenant ID - in a real app, this would come from configuration or environment
const DEFAULT_TENANT_ID = 'default-tenant';

async function fetchTasks() {
  try {
    const response = await fetch(`${ORCHESTRATOR_URL}/tasks`, {
      headers: {
        'X-Tenant-ID': DEFAULT_TENANT_ID,
        'Content-Type': 'application/json'
      }
    });
    const tasks = await response.json();
    
    // Handle case where tasks is not an array
    if (!Array.isArray(tasks)) {
      console.warn('Received non-array response from tasks endpoint:', tasks);
      return [];
    }
    
    return tasks.filter(task => task.status === 'pending');
  } catch (error) {
    console.error('Error fetching tasks:', error);
    return [];
  }
}

async function claimTask(taskId) {
  try {
    const response = await fetch(`${ORCHESTRATOR_URL}/tasks/${taskId}/claim`, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'X-Tenant-ID': DEFAULT_TENANT_ID
      }
    });
    return response.ok;
  } catch (error) {
    console.error('Error claiming task:', error);
    return false;
  }
}

async function updateTaskStatus(taskId, status) {
  try {
    await fetch(`${ORCHESTRATOR_URL}/tasks/${taskId}/status`, {
      method: 'PATCH',
      headers: { 
        'Content-Type': 'application/json',
        'X-Tenant-ID': DEFAULT_TENANT_ID
      },
      body: JSON.stringify({ status })
    });
  } catch (error) {
    console.error('Error updating task status:', error);
  }
}

async function simulateWork(taskId) {
  console.log(`Processing task ${taskId}...`);
  await new Promise(resolve => setTimeout(resolve, 2000)); // Simulate 2s of work
  console.log(`Completed task ${taskId}`);
}

async function processTask(task) {
  const claimed = await claimTask(task.id);
  if (!claimed) return;

  await simulateWork(task.id);
  await updateTaskStatus(task.id, 'completed');
}

async function pollAndProcess() {
  while (true) {
    const tasks = await fetchTasks();
    for (const task of tasks) {
      await processTask(task);
    }
    await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL));
  }
}

// Start the executor
console.log('Claude executor starting...');
console.log(`Connecting to orchestrator at ${ORCHESTRATOR_URL}`);
pollAndProcess().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
}); 