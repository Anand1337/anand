pub struct FastDistribution {
    min_val: i32,
    max_val: i32,
    count: Vec<u64>,
    total_count: u64,
}

impl FastDistribution {
    pub fn new(min_val: i32, max_val: i32) -> Self {
        assert!(min_val <= max_val, "Incorrect distribution range: {} {}", min_val, max_val);
        let size = (max_val - min_val + 1) as usize;
        assert!(size <= 10_001, "Too big range for fast distribution: {} {}", min_val, max_val);
        FastDistribution {
            min_val: min_val,
            max_val: max_val,
            count: vec![0; size],
            total_count: 0,
        }
    }

    pub fn add(&mut self, value: i32) -> Result<(), String> {
        if value < self.min_val || value > self.max_val {
            return Err(format!(
                "The value: {} doesn't belong to the distribution range: {} {}",
                value, self.min_val, self.max_val
            ));
        }
        let index = (value - self.min_val) as usize;
        self.count[index] += 1;
        self.total_count += 1;
        Ok(())
    }

    pub fn clear(&mut self) {
        self.count = vec![0; self.count.len()];
        self.total_count = 0;
    }

    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    pub fn get_distribution(&self, percentiles: &Vec<f32>) -> Vec<(f32, i32)> {
        let mut indexes: Vec<u64> = vec![0; percentiles.len()];
        for i in 0..percentiles.len() {
            indexes[i] = (self.total_count as f64 * percentiles[i] as f64 / 100.) as u64;
        }

        let mut result: Vec<(f32, i32)> = vec![(0., 0); percentiles.len()];
        let mut index: usize = 0;
        let mut sum: u64 = 0;
        for i in 0..self.count.len() {
            sum += self.count[i];
            while index < indexes.len() && sum >= indexes[index] {
                result[index] = (percentiles[index], self.min_val + i as i32);
                index += 1;
            }
        }
        while index < indexes.len() {
            result[index] = (percentiles[index], self.max_val);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::FastDistribution;

    fn add_range(dist: &mut FastDistribution, start: i32, end: i32) {
        for i in start..end {
            dist.add(i).unwrap();
        }
    }

    #[test]
    fn test_1_to_100() {
        let mut dist = FastDistribution::new(1, 100);
        add_range(&mut dist, 1, 101);
        let expected = vec![(50., 50), (90., 90), (95., 95), (99., 99)];
        let received = dist.get_distribution(&vec![50., 90., 95., 99.]);
        assert_eq!(received, expected);
    }

    #[test]
    fn test_1_to_100_twice() {
        let mut dist = FastDistribution::new(1, 100);
        add_range(&mut dist, 1, 101);
        add_range(&mut dist, 1, 101);
        let expected = vec![(50., 50), (90., 90), (95., 95), (99., 99)];
        let received = dist.get_distribution(&vec![50., 90., 95., 99.]);
        assert_eq!(received, expected);
    }

    #[test]
    fn test_1_to_100_large_array() {
        let mut dist = FastDistribution::new(-5000, 4999);
        add_range(&mut dist, 1, 101);
        let expected = vec![(50., 50), (90., 90), (95., 95), (99., 99)];
        let received = dist.get_distribution(&vec![50., 90., 95., 99.]);
        assert_eq!(received, expected);
    }

    #[test]
    fn test_minus_99_to_100() {
        let mut dist = FastDistribution::new(-100, 100);
        add_range(&mut dist, -99, 101);
        let expected = vec![
            (1.0, -98),
            (5.0, -90),
            (10.0, -80),
            (50.0, 0),
            (90.0, 80),
            (95.0, 90),
            (99.0, 98),
        ];
        let received = dist.get_distribution(&vec![1., 5., 10., 50., 90., 95., 99.]);
        assert_eq!(received, expected);
    }

    #[test]
    fn test_total_count() {
        let mut dist = FastDistribution::new(1, 100);
        add_range(&mut dist, 1, 101);
        assert_eq!(dist.total_count(), 100);
    }

    #[test]
    fn test_clear() {
        let percentiles = vec![50., 90., 95., 99.];

        let mut dist = FastDistribution::new(1, 200);
        add_range(&mut dist, 1, 101);
        assert_eq!(
            dist.get_distribution(&percentiles),
            vec![(50., 50), (90., 90), (95., 95), (99., 99)]
        );
        dist.clear();
        add_range(&mut dist, 101, 201);
        assert_eq!(
            dist.get_distribution(&percentiles),
            vec![(50., 150), (90., 190), (95., 195), (99., 199)]
        );
    }

    #[test]
    #[should_panic]
    fn test_incorrect_range() {
        FastDistribution::new(101, 100);
    }

    #[test]
    #[should_panic]
    fn test_too_big_range() {
        FastDistribution::new(0, 10_000);
    }

    #[test]
    fn test_empty() {
        let dist = FastDistribution::new(1, 100);
        let expected = vec![(50., 1), (90., 1), (95., 1), (99., 1)];
        let received = dist.get_distribution(&vec![50., 90., 95., 99.]);
        assert_eq!(received, expected);
    }

    #[test]
    fn test_size_1() {
        let mut dist = FastDistribution::new(1, 100);
        add_range(&mut dist, 10, 11);
        let expected = vec![(50., 1), (90., 1), (95., 1), (99., 1)];
        let received = dist.get_distribution(&vec![50., 90., 95., 99.]);
        assert_eq!(received, expected);
    }

    #[test]
    fn test_size_2() {
        let mut dist = FastDistribution::new(1, 100);
        add_range(&mut dist, 10, 12);
        let expected = vec![(50., 10), (90., 10), (95., 10), (99., 10)];
        let received = dist.get_distribution(&vec![50., 90., 95., 99.]);
        assert_eq!(received, expected);
    }
}
