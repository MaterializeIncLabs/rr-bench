use std::fmt;
use std::time::Duration;

pub struct PrettyDuration(Duration);

impl From<Duration> for PrettyDuration {
    fn from(value: Duration) -> Self {
        Self(value)
    }
}

impl From<PrettyDuration> for Duration {
    fn from(val: PrettyDuration) -> Self {
        val.0
    }
}

impl fmt::Display for PrettyDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let millis = self.0.as_secs_f64() * 1000.0;
        write!(f, "{:.6} ms", millis)
    }
}
