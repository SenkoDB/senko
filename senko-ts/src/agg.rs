#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregation {
    Avg,
    First,
    Last,
    Min,
    Max,
    Sum,
    Range,
    Count,
    StdP,
    StdS,
    VarP,
    VarS,
    Twa,
}

#[derive(Debug, Clone, Default)]
pub struct Aggregator {
    first: Option<f64>,
    last: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
    sum: f64,
    count: u64,
    mean: f64,
    m2: f64,
    twa_area: f64,
    prev_sample: Option<(i64, f64)>,
}

impl Aggregator {
    pub fn push(&mut self, ts: i64, value: f64) {
        self.first.get_or_insert(value);
        self.last = Some(value);
        self.min = Some(self.min.map_or(value, |current| current.min(value)));
        self.max = Some(self.max.map_or(value, |current| current.max(value)));
        self.sum += value;
        self.count += 1;

        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;

        if let Some((prev_ts, prev_val)) = self.prev_sample {
            let span = (ts - prev_ts).max(0) as f64;
            self.twa_area += ((prev_val + value) * 0.5) * span;
        }
        self.prev_sample = Some((ts, value));
    }

    pub fn value(
        &self,
        aggregation: Aggregation,
        bucket_start: i64,
        bucket_end: i64,
    ) -> Option<f64> {
        match aggregation {
            Aggregation::Avg => (self.count > 0).then_some(self.sum / self.count as f64),
            Aggregation::First => self.first,
            Aggregation::Last => self.last,
            Aggregation::Min => self.min,
            Aggregation::Max => self.max,
            Aggregation::Sum => (self.count > 0).then_some(self.sum),
            Aggregation::Range => match (self.min, self.max) {
                (Some(min), Some(max)) => Some(max - min),
                _ => None,
            },
            Aggregation::Count => Some(self.count as f64),
            Aggregation::StdP => (self.count > 0).then_some((self.m2 / self.count as f64).sqrt()),
            Aggregation::StdS => {
                (self.count > 1).then_some((self.m2 / (self.count - 1) as f64).sqrt())
            }
            Aggregation::VarP => (self.count > 0).then_some(self.m2 / self.count as f64),
            Aggregation::VarS => (self.count > 1).then_some(self.m2 / (self.count - 1) as f64),
            Aggregation::Twa => {
                let span = (bucket_end - bucket_start).max(0) as f64;
                if self.count == 0 || span == 0.0 {
                    None
                } else if self.count == 1 {
                    self.first
                } else {
                    Some(self.twa_area / span)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregation, Aggregator};

    #[test]
    fn avg_and_count_work() {
        let mut agg = Aggregator::default();
        agg.push(0, 1.0);
        agg.push(10, 3.0);
        agg.push(20, 5.0);
        assert_eq!(agg.value(Aggregation::Avg, 0, 20), Some(3.0));
        assert_eq!(agg.value(Aggregation::Count, 0, 20), Some(3.0));
    }
}
