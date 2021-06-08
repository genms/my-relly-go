package memcmpable

const ESCAPE_LENGTH int = 9

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func EncodedSize(length int) int {
	return (length + (ESCAPE_LENGTH - 1)) / (ESCAPE_LENGTH - 1) * ESCAPE_LENGTH
}

func Encode(src []byte, dst []byte) []byte {
	for {
		copyLen := min(ESCAPE_LENGTH-1, len(src))
		dst = append(dst, src[0:copyLen]...)
		src = src[copyLen:]
		if len(src) == 0 {
			padSize := ESCAPE_LENGTH - 1 - copyLen
			if padSize > 0 {
				dst = append(dst, make([]byte, padSize)...)
			}
			dst = append(dst, byte(copyLen))
			break
		}
		dst = append(dst, byte(ESCAPE_LENGTH))
	}
	return dst
}

func Decode(src []byte, dst []byte) ([]byte, []byte) {
	for {
		extra := src[ESCAPE_LENGTH-1]
		length := min(ESCAPE_LENGTH-1, int(extra))
		dst = append(dst, src[:length]...)
		src = src[ESCAPE_LENGTH:]
		if extra < byte(ESCAPE_LENGTH) {
			break
		}
	}
	return src, dst
}
