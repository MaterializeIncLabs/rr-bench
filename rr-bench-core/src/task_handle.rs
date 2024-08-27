use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub fn new_task_handles() -> (TaskHandle, CompletionTracker) {
    let count = Arc::new(AtomicUsize::new(1));
    (
        TaskHandle {
            count: count.clone(),
        },
        CompletionTracker { count },
    )
}

pub struct TaskHandle {
    count: Arc<AtomicUsize>,
}

impl Clone for TaskHandle {
    fn clone(&self) -> Self {
        self.count.fetch_add(1, Ordering::SeqCst);
        Self {
            count: self.count.clone(),
        }
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct CompletionTracker {
    count: Arc<AtomicUsize>,
}

impl CompletionTracker {
    pub fn is_done(&self) -> bool {
        self.count.load(Ordering::SeqCst) == 0
    }
}
