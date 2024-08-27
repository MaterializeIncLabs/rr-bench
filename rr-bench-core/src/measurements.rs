use std::fmt;
use std::time::Duration;

pub struct Measurements {
    durations: Vec<Duration>,
    total_duration: Duration,
}

impl Measurements {
    pub fn new(total_duration: Duration) -> Self {
        Self {
            durations: Vec::new(),
            total_duration,
        }
    }

    pub fn push(&mut self, value: Duration) {
        self.durations.push(value)
    }

    pub fn total_transactions(&self) -> usize {
        self.durations.len()
    }

    pub fn tps(&self) -> f64 {
        self.total_transactions() as f64 / self.total_duration.as_secs_f64()
    }

    pub fn max(&self) -> humantime::Duration {
        (*self.durations.iter().max().unwrap()).into()
    }

    pub fn min(&self) -> humantime::Duration {
        (*self.durations.iter().min().unwrap()).into()
    }

    pub fn average(&self) -> humantime::Duration {
        let total_duration: Duration = self.durations.iter().cloned().sum();
        (total_duration / self.total_transactions() as u32).into()
    }

    pub fn median(&self) -> humantime::Duration {
        let mut sorted = self.durations.clone();
        sorted.sort();

        let mid = sorted.len() / 2;
        let median = if sorted.len() % 2 == 0 {
            (sorted[mid - 1] + sorted[mid]) / 2
        } else {
            sorted[mid]
        };

        median.into()
    }

    pub fn standard_deviation(&self) -> humantime::Duration {
        let avg_secs: Duration = self.average().into();
        let avg_secs = avg_secs.as_secs_f64();
        let variance: f64 = self
            .durations
            .iter()
            .map(|&duration| {
                let diff_secs = duration.as_secs_f64() - avg_secs;
                diff_secs.powi(2)
            })
            .sum::<f64>()
            / self.total_transactions() as f64;

        let stddev_secs = variance.sqrt();
        Duration::from_secs_f64(stddev_secs).into()
    }

    pub fn percentile_95(&self) -> humantime::Duration {
        self.percentile(95)
    }

    pub fn percentile_99(&self) -> humantime::Duration {
        self.percentile(95)
    }

    pub fn percentile(&self, percentile: usize) -> humantime::Duration {
        let mut sorted = self.durations.clone();
        sorted.sort();

        let idx = ((percentile as f64 / 100.0) * sorted.len() as f64).ceil() as usize - 1;
        sorted[idx.min(sorted.len() - 1)].into()
    }
}

impl fmt::Display for Measurements {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Total Transactions: {}", self.total_transactions())?;
        writeln!(f, "Transactions per Second (TPS): {:.2}", self.tps())?;
        writeln!(f, "Max Latency: {}", self.max())?;
        writeln!(f, "Min Latency: {}", self.min())?;
        writeln!(f, "Average Latency: {}", self.average())?;
        writeln!(f, "Median Latency: {}", self.median())?;
        writeln!(f, "95th Percentile Latency: {}", self.percentile_95())?;
        writeln!(f, "99th Percentile Latency: {}", self.percentile_99())?;
        writeln!(f, "Standard Deviation: {}", self.standard_deviation())
    }
}
