{
	max = $(3 + $2)
	for (i = 3; i <= NF; i++) {
		printf "%s", (max != 0 ? $i/max : 0) (i == NF ? ORS : OFS)
	}
}
