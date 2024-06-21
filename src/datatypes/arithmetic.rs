use super::NoirType;

// ----- Addition -----

impl<T: Into<NoirType>> std::ops::Add<T> for NoirType {
    type Output = NoirType;

    fn add(self, other: T) -> Self::Output {
        match self {
            NoirType::Int(i) => match other.into() {
                NoirType::Int(j) => NoirType::Int(i + j),
                NoirType::Float(j) => NoirType::Float(i as f32 + j),
                other => panic!("Invalid addition operation ==> {} + {}", self, other),
            },
            NoirType::Float(i) => match other.into() {
                NoirType::Int(j) => NoirType::Float(i + j as f32),
                NoirType::Float(j) => NoirType::Float(i + j),
                other => panic!("Invalid addition operation ==> {} + {}", self, other),
            },
            NoirType::String(i) => match other.into() {
                NoirType::String(j) => NoirType::String(i + &j),
                other => panic!(
                    "Invalid addition operation ==> {} + {}",
                    NoirType::String(i),
                    other
                ),
            },
            _ => panic!("Invalid addition operation ==> {} + {}", self, other.into()),
        }
    }
}

impl<T: Into<NoirType>> std::ops::AddAssign<T> for NoirType {
    fn add_assign(&mut self, rhs: T) {
        *self = self.clone() + rhs;
    }
}

// ----- Subtraction -----

impl<T: Into<NoirType>> std::ops::Sub<T> for NoirType {
    type Output = NoirType;

    fn sub(self, other: T) -> Self::Output {
        match self {
            NoirType::Int(i) => match other.into() {
                NoirType::Int(j) => NoirType::Int(i - j),
                NoirType::Float(j) => NoirType::Float(i as f32 - j),
                other => panic!("Invalid subtraction operation ==> {} - {}", self, other),
            },
            NoirType::Float(i) => match other.into() {
                NoirType::Int(j) => NoirType::Float(i - j as f32),
                NoirType::Float(j) => NoirType::Float(i - j),
                other => panic!("Invalid subtraction operation ==> {} - {}", self, other),
            },
            _ => panic!(
                "Invalid subtraction operation ==> {} - {}",
                self,
                other.into()
            ),
        }
    }
}

impl<T: Into<NoirType>> std::ops::SubAssign<T> for NoirType {
    fn sub_assign(&mut self, rhs: T) {
        *self = self.clone() - rhs;
    }
}

// ----- Multiplication -----

impl<T: Into<NoirType>> std::ops::Mul<T> for NoirType {
    type Output = NoirType;

    fn mul(self, other: T) -> Self::Output {
        match self {
            NoirType::Int(i) => match other.into() {
                NoirType::Int(j) => NoirType::Int(i * j),
                NoirType::Float(j) => NoirType::Float(i as f32 * j),
                other => panic!("Invalid multiplication operation ==> {} * {}", self, other),
            },
            NoirType::Float(i) => match other.into() {
                NoirType::Int(j) => NoirType::Float(i * j as f32),
                NoirType::Float(j) => NoirType::Float(i * j),
                other => panic!("Invalid multiplication operation ==> {} * {}", self, other),
            },
            _ => panic!(
                "Invalid multiplication operation ==> {} * {}",
                self,
                other.into()
            ),
        }
    }
}

impl<T: Into<NoirType>> std::ops::MulAssign<T> for NoirType {
    fn mul_assign(&mut self, rhs: T) {
        *self = self.clone() * rhs;
    }
}

// ----- Division -----

impl<T: Into<NoirType>> std::ops::Div<T> for NoirType {
    type Output = NoirType;

    fn div(self, other: T) -> Self::Output {
        match self {
            NoirType::Int(i) => match other.into() {
                NoirType::Int(j) => NoirType::Int(i / j),
                NoirType::Float(j) => NoirType::Float(i as f32 / j),
                other => panic!("Invalid division operation ==> {} / {}", self, other),
            },
            NoirType::Float(i) => match other.into() {
                NoirType::Int(j) => NoirType::Float(i / j as f32),
                NoirType::Float(j) => NoirType::Float(i / j),
                other => panic!("Invalid division operation ==> {} / {}", self, other),
            },
            _ => panic!("Invalid division operation ==> {} / {}", self, other.into()),
        }
    }
}

impl<T: Into<NoirType>> std::ops::DivAssign<T> for NoirType {
    fn div_assign(&mut self, rhs: T) {
        *self = self.clone() / rhs;
    }
}
