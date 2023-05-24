/// ranges are inclusive on both sides
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

#[cfg(test)]
mod test {
	use crate::utils::make_descending_ranges;

	#[test]
	fn test_make_descending_ranges() {
		let res = make_descending_ranges(vec![3, 1, 2, 7, 9, 11, 10]);
		assert_eq!(res, vec![(11, 9), (7, 7), (3, 1)]);
	}
}
