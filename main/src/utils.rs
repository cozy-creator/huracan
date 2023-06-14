/// ranges are inclusive on both sides
#[allow(unused)]
pub fn make_descending_ranges(mut numbers: Vec<u64>) -> Vec<(u64, u64)> {
	if numbers.is_empty() {
		return Vec::new()
	}
	numbers.sort_unstable();
	let mut start = numbers[0];
	let mut end = numbers[0];
	let mut ranges = Vec::new();
	for &n in numbers.iter().skip(1) {
		if n == end + 1 {
			end = n;
		} else {
			ranges.push((end, start));
			start = n;
			end = n;
		}
	}
	ranges.push((end, start));
	ranges.reverse();
	ranges
}

/// ranges are inclusive on both sides; tuples must be sorted.
pub fn make_descending_ranges_tuples(mut numbers: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
	if numbers.is_empty() {
		return Vec::new()
	}
	numbers.sort_unstable();
	let (mut x1, mut x2) = numbers[0];
	let mut ranges = Vec::new();
	for &(y1, y2) in numbers.iter().skip(1) {
		// this comparison is enough because we're expecting tuples to be sorted, aka x1<=x2 && y1<=y2
		if y1 <= x2 + 1 {
			x2 = x2.max(y2);
		} else {
			ranges.push((x2, x1));
			x1 = y1;
			x2 = y2;
		}
	}
	ranges.push((x2, x1));
	ranges.reverse();
	ranges
}

#[cfg(test)]
mod test {
	use crate::utils::{make_descending_ranges, make_descending_ranges_tuples};

	#[test]
	fn test_make_descending_ranges() {
		let res = make_descending_ranges(vec![3, 1, 2, 7, 9, 11, 10]);
		assert_eq!(res, vec![(11, 9), (7, 7), (3, 1)]);
	}

	#[test]
	fn test_make_descending_ranges_tuples() {
		let res = make_descending_ranges_tuples(vec![(3, 3), (1, 1), (2, 2), (7, 7), (9, 9), (11, 11), (10, 10)]);
		assert_eq!(res, vec![(11, 9), (7, 7), (3, 1)]);

		let res = make_descending_ranges_tuples(vec![(3, 3), (1, 3), (2, 2), (7, 7), (9, 11), (13, 13)]);
		assert_eq!(res, vec![(13, 13), (11, 9), (7, 7), (3, 1)]);

		let res = make_descending_ranges_tuples(vec![(1, 2), (1, 4), (2, 3)]);
		assert_eq!(res, vec![(4, 1)]);
	}
}
