pub struct Arena<T> {
    data: Vec<T>,
    next: usize,
}

impl<T> Default for Arena<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Arena<T> {
    pub fn new() -> Self {
        Arena {
            data: Vec::new(),
            next: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Arena {
            data: Vec::with_capacity(capacity),
            next: 0,
        }
    }

    pub fn alloc(&mut self, value: T) -> usize {
        let index = self.next;
        self.next += 1;
        self.data.push(value);
        index
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.data.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.data.get_mut(index)
    }
}
