use super::NoirType;

impl<T: Into<NoirType>> std::ops::Rem<T> for NoirType {
    type Output = NoirType;

    fn rem(self, rhs: T) -> Self::Output {
        let rhs = rhs.into();
        match (self, rhs) {
            (NoirType::Int(lhs), NoirType::Int(rhs)) => NoirType::Int(lhs % rhs),
            (NoirType::Float(lhs), NoirType::Float(rhs)) => NoirType::Float(lhs % rhs),
            (x, y) => panic!("Cannot perform modulo operation on {:?} and {:?}", x, y),
        }
    }
}

impl<T: Into<NoirType>> std::ops::BitXor<T> for NoirType {
    type Output = NoirType;

    fn bitxor(self, rhs: T) -> Self::Output {
        let rhs = rhs.into();
        match (self, rhs) {
            (NoirType::Int(lhs), NoirType::Int(rhs)) => NoirType::Int(lhs ^ rhs),
            (NoirType::Float(lhs), NoirType::Float(rhs)) => {
                NoirType::Float((lhs.to_bits() ^ rhs.to_bits()) as f32)
            }
            (NoirType::Bool(lhs), NoirType::Bool(rhs)) => NoirType::Bool(lhs ^ rhs),
            (x, y) => panic!(
                "Cannot perform bitwise xor operation on {:?} and {:?}",
                x, y
            ),
        }
    }
}

impl std::ops::Not for NoirType {
    type Output = NoirType;

    fn not(self) -> Self::Output {
        let value: bool = self.into();
        NoirType::Bool(!value)
    }
}
