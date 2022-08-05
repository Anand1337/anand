/// to specify a part we always specify both part_id and num_parts together
#[derive(Copy, Clone)]
pub struct PartId {
    part_id: u64,
    num_parts: u64,
}

impl PartId {
    pub fn new(part_id: u64, num_parts: u64) -> Self {
        assert!(part_id < num_parts);
        Self { part_id, num_parts }
    }

    pub fn get_part_id(&self) -> u64 {
        self.part_id
    }

    pub fn get_num_parts(&self) -> u64 {
        self.num_parts
    }
}
