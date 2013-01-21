
describe("Numeric", function() {
	it("returns 0 when the number is 0", function() {
		(0).should.equal(0);
	});

	it("returns the sum of numeric elements", function() {
		(1 + 2 + 3).should.equal(6);
	});
});
