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
		SET_ELEMENT(colVec, col, NEW_STRING(rows));
	}

	int cRow;
	int cCol;
	int tokenStart;
	int curPos;
	int endQuote;

	int bsize = 1024;
	char *valPtr = (char*) malloc(bsize * sizeof(char));
	if (valPtr == NULL) {
		printf("malloc() failed. Are you running out of memory? [%s]\n",
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
					int tokenLen = curPos - tokenStart - endQuote;
					if (tokenLen < 1) {
						printf("parsing error in '%s'\n", val);
						return colVec;
					}

					// check if token fits in buffer, if not, realloc
					while (tokenLen >= bsize) {
						bsize *= 2;
						valPtr = realloc(valPtr, bsize * sizeof(char*));
						if (valPtr == NULL) {
							printf(
									"malloc() failed. Are you running out of memory? [%s]\n",
									strerror(errno));
							return colVec;
						}
					}

					strncpy(valPtr, val + tokenStart, tokenLen);
					valPtr[tokenLen] = '\0';

					assert(cCol < numCols);
					SEXP colV = VECTOR_ELT(colVec, cCol);

					if (strcmp(valPtr, nullstr) == 0) {
						SET_STRING_ELT(colV, cRow, NA_STRING);

					} else {
						SEXP rval = mkChar(valPtr);
						assert(rval != NULL);
						assert (IS_CHARACTER( rval));
						SET_STRING_ELT(colV, cRow, rval);
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

SEXP mapiSplitLines(SEXP mapiString) {

	PROTECT(mapiString = AS_CHARACTER(mapiString));
	assert(LENGTH(mapiString) == 1);

	size_t arr_length = 100;
	size_t arr_elements = 0;

	char **array = (char**) malloc(arr_length * sizeof(char*));
	const char *val = CHAR(STRING_ELT(mapiString, 0));
	char *line;
	char sep[] = "\n";

	line = strtok((char * restrict) val, sep);
	while (line != NULL) {
		if (arr_elements >= arr_length) {
			arr_length *= 2;
			array = realloc(array, arr_length * sizeof(char*));
		}
		array[arr_elements] = line;
		arr_elements++;

		line = strtok(NULL, sep);
	}
	SEXP lineVec;
	PROTECT(lineVec = NEW_STRING(arr_elements));

	size_t curLine;
	for (curLine = 0; curLine < arr_elements; curLine++) {
		SEXP rval = mkChar(array[curLine]);
		assert(rval != NULL);
		assert (IS_CHARACTER( rval));
		SET_STRING_ELT(lineVec, curLine, rval);
	}

	UNPROTECT(2);
	return lineVec;
}
