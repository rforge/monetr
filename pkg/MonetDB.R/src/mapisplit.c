#include <R.h>
#include <Rdefines.h>
#include <assert.h>
#include <string.h>
#include <errno.h>

typedef enum {
	INQUOTES, ESCAPED, NORMAL
} chrstate;

char nullstr[] = "NULL";

SEXP mapiSplit(SEXP mapiLinesVector, SEXP numCols) {
	PROTECT(mapiLinesVector = AS_CHARACTER(mapiLinesVector));

	int cols = INTEGER_POINTER(AS_INTEGER(numCols))[0];
	int rows = LENGTH(mapiLinesVector);

	assert(rows > 0);
	assert(cols > 0);

	SEXP colVec;
	PROTECT(colVec = NEW_LIST(cols));

	int col;
	for (col = 0; col < cols; col++) {
		SEXP colV = PROTECT(NEW_STRING(rows));
		assert(TYPEOF(colV) == STRSXP);
		SET_ELEMENT(colVec, col, colV);
		UNPROTECT(1);
	}

	int cRow;
	int cCol;
	int tokenStart;
	int curPos;
	int endQuote;

	int bsize = 1024;
	char *valPtr = (char*) malloc(bsize * sizeof(char));
	if (valPtr == NULL) {
		error("malloc() failed. Are you running out of memory? [%s]\n",
				strerror(errno));
		return colVec;
	}

	for (cRow = 0; cRow < rows; cRow++) {
		const char *val = CHAR(STRING_ELT(mapiLinesVector, cRow));
		int linelen = LENGTH(STRING_ELT(mapiLinesVector, cRow));

		cCol = 0;
		tokenStart = 2;
		curPos = 0;
		endQuote = 0;

		chrstate state = NORMAL;

		for (curPos = 2; curPos < linelen - 1; curPos++) {
			char chr = val[curPos];

			switch (state) {
			case NORMAL:
				if (chr == '"') {
					state = INQUOTES;
					tokenStart++;
					break;
				}
				if (chr == ',' || curPos == linelen - 2) {
					if (cCol >= cols) {
						error("Too many columns in '%s', max %d, have %d", val,
								cols, cCol);
					}
					assert(cCol < cols);

					int tokenLen = curPos - tokenStart - endQuote;

					// check if token fits in buffer, if not, realloc
					while (tokenLen >= bsize) {
						bsize *= 2;
						valPtr = realloc(valPtr, bsize * sizeof(char*));
						if (valPtr == NULL) {
							error(
									"realloc() failed. Are you running out of memory? [%s]\n",
									strerror(errno));
							return colVec;
						}
					}

					strncpy(valPtr, val + tokenStart, tokenLen);
					valPtr[tokenLen] = '\0';

					assert(cCol < numCols);

					SEXP colV = VECTOR_ELT(colVec, cCol);

					assert(TYPEOF(colV) == STRSXP);

					if (tokenLen < 1 || strcmp(valPtr, nullstr) == 0) {
						SET_STRING_ELT(colV, cRow, NA_STRING);

					} else {
						SET_STRING_ELT(colV, cRow, mkCharLen(valPtr, tokenLen));
					}
					cCol++;
					tokenStart = curPos + 2;
					endQuote = 0;
				}

				break;

			case ESCAPED:
				state = INQUOTES;
				break;
			case INQUOTES:
				if (chr == '"') {
					state = NORMAL;
					endQuote++;
					break;
				}
				if (chr == '\\') {
					state = ESCAPED;
					break;
				}
				break;
			}
		}
	}
	free(valPtr);

	UNPROTECT(2);
	return colVec;
}
