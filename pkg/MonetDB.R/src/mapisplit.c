#include <R.h>
#include <Rdefines.h>
#include <assert.h>
#include <string.h>

typedef enum {
	INQUOTES, ESCAPED, NORMAL
} chrstate;

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
		SET_ELEMENT(colVec, col, NEW_STRING(rows));
	}

	int cRow;
	int cCol;
	int tokenStart;
	int curPos;
	int endQuote;

	for (cRow = 0; cRow < rows; cRow++) {
		const char *val = CHAR(STRING_ELT(mapiLinesVector, cRow));
		int strlen = LENGTH(STRING_ELT(mapiLinesVector, cRow));

		cCol = 0;
		tokenStart = 2;
		curPos = 0;
		endQuote = 0;

		chrstate state = NORMAL;

		for (curPos = 2; curPos < strlen - 1; curPos++) {
			char chr = val[curPos];

			switch (state) {
			case NORMAL:
				if (chr == '"') {
					state = INQUOTES;
					tokenStart++;
					break;
				}
				if (chr == ',' || curPos == strlen - 2) {
					int tokenLen = curPos - tokenStart + 1 - endQuote;
					char *valPtr = (char*) malloc(tokenLen * sizeof(char));

					strncpy(valPtr, val + tokenStart, tokenLen);
					valPtr[tokenLen - 1] = '\0';

					SEXP colV = VECTOR_ELT(colVec, cCol);
					SET_STRING_ELT(colV, cRow, mkChar(valPtr));

					free(valPtr);

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

	UNPROTECT(2);
	return colVec;
}
