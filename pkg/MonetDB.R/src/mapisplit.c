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
					int tokenLen = curPos - tokenStart + 1 - endQuote;
					if (tokenLen < 1) {
						printf("parsing error in '%s'\n", val);
						return colVec;
					}
					char *valPtr = (char*) malloc(tokenLen * sizeof(char));
					if (valPtr == NULL) {
						printf(
								"malloc() failed. Are you running out of memory? [%s]\n",
								strerror(errno));
						return colVec;
					}
					strncpy(valPtr, val + tokenStart, tokenLen);
					valPtr[tokenLen - 1] = '\0';
					SEXP colV = VECTOR_ELT(colVec, cCol);

					if (strcmp(valPtr, nullstr) == 0) {
						SET_STRING_ELT(colV, cRow, NA_STRING);

					} else {
						SET_STRING_ELT(colV, cRow, mkChar(valPtr));
					}

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
		SET_STRING_ELT(lineVec, curLine, mkChar(array[curLine]));
	}

	UNPROTECT(2);
	return lineVec;
}
