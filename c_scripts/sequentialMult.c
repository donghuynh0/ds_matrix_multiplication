#include <stdio.h>
#include <stdlib.h>
#include <time.h>

// Function to allocate a matrix
int** allocateMatrix(int n) {
    int** mat = (int**)malloc(n * sizeof(int*));
    for (int i = 0; i < n; i++) {
        mat[i] = (int*)malloc(n * sizeof(int));
    }
    return mat;
}

// Function to free a matrix
void freeMatrix(int** mat, int n) {
    for (int i = 0; i < n; i++) {
        free(mat[i]);
    }
    free(mat);
}

// Function to add two matrices
void addMatrix(int** A, int** B, int** C, int n) {
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            C[i][j] = A[i][j] + B[i][j];
        }
    }
}

// Function to subtract two matrices
void subtractMatrix(int** A, int** B, int** C, int n) {
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            C[i][j] = A[i][j] - B[i][j];
        }
    }
}

// Standard matrix multiplication (for base case)
void standardMultiply(int** A, int** B, int** C, int n) {
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            C[i][j] = 0;
            for (int k = 0; k < n; k++) {
                C[i][j] += A[i][k] * B[k][j];
            }
        }
    }
}

// Strassen's algorithm for matrix multiplication
void strassenMultiply(int** A, int** B, int** C, int n) {
    // Base case: use standard multiplication for small matrices
    if (n <= 64) {
        standardMultiply(A, B, C, n);
        return;
    }
    
    int newSize = n / 2;
    
    // Allocate submatrices
    int** A11 = allocateMatrix(newSize);
    int** A12 = allocateMatrix(newSize);
    int** A21 = allocateMatrix(newSize);
    int** A22 = allocateMatrix(newSize);
    
    int** B11 = allocateMatrix(newSize);
    int** B12 = allocateMatrix(newSize);
    int** B21 = allocateMatrix(newSize);
    int** B22 = allocateMatrix(newSize);
    
    int** C11 = allocateMatrix(newSize);
    int** C12 = allocateMatrix(newSize);
    int** C21 = allocateMatrix(newSize);
    int** C22 = allocateMatrix(newSize);
    
    int** M1 = allocateMatrix(newSize);
    int** M2 = allocateMatrix(newSize);
    int** M3 = allocateMatrix(newSize);
    int** M4 = allocateMatrix(newSize);
    int** M5 = allocateMatrix(newSize);
    int** M6 = allocateMatrix(newSize);
    int** M7 = allocateMatrix(newSize);
    
    int** temp1 = allocateMatrix(newSize);
    int** temp2 = allocateMatrix(newSize);
    
    // Divide matrices into quadrants
    for (int i = 0; i < newSize; i++) {
        for (int j = 0; j < newSize; j++) {
            A11[i][j] = A[i][j];
            A12[i][j] = A[i][j + newSize];
            A21[i][j] = A[i + newSize][j];
            A22[i][j] = A[i + newSize][j + newSize];
            
            B11[i][j] = B[i][j];
            B12[i][j] = B[i][j + newSize];
            B21[i][j] = B[i + newSize][j];
            B22[i][j] = B[i + newSize][j + newSize];
        }
    }
    
    // Calculate M1 = (A11 + A22) * (B11 + B22)
    addMatrix(A11, A22, temp1, newSize);
    addMatrix(B11, B22, temp2, newSize);
    strassenMultiply(temp1, temp2, M1, newSize);
    
    // Calculate M2 = (A21 + A22) * B11
    addMatrix(A21, A22, temp1, newSize);
    strassenMultiply(temp1, B11, M2, newSize);
    
    // Calculate M3 = A11 * (B12 - B22)
    subtractMatrix(B12, B22, temp2, newSize);
    strassenMultiply(A11, temp2, M3, newSize);
    
    // Calculate M4 = A22 * (B21 - B11)
    subtractMatrix(B21, B11, temp2, newSize);
    strassenMultiply(A22, temp2, M4, newSize);
    
    // Calculate M5 = (A11 + A12) * B22
    addMatrix(A11, A12, temp1, newSize);
    strassenMultiply(temp1, B22, M5, newSize);
    
    // Calculate M6 = (A21 - A11) * (B11 + B12)
    subtractMatrix(A21, A11, temp1, newSize);
    addMatrix(B11, B12, temp2, newSize);
    strassenMultiply(temp1, temp2, M6, newSize);
    
    // Calculate M7 = (A12 - A22) * (B21 + B22)
    subtractMatrix(A12, A22, temp1, newSize);
    addMatrix(B21, B22, temp2, newSize);
    strassenMultiply(temp1, temp2, M7, newSize);
    
    // Calculate C quadrants
    // C11 = M1 + M4 - M5 + M7
    addMatrix(M1, M4, temp1, newSize);
    subtractMatrix(temp1, M5, temp2, newSize);
    addMatrix(temp2, M7, C11, newSize);
    
    // C12 = M3 + M5
    addMatrix(M3, M5, C12, newSize);
    
    // C21 = M2 + M4
    addMatrix(M2, M4, C21, newSize);
    
    // C22 = M1 - M2 + M3 + M6
    subtractMatrix(M1, M2, temp1, newSize);
    addMatrix(temp1, M3, temp2, newSize);
    addMatrix(temp2, M6, C22, newSize);
    
    // Combine quadrants into result matrix
    for (int i = 0; i < newSize; i++) {
        for (int j = 0; j < newSize; j++) {
            C[i][j] = C11[i][j];
            C[i][j + newSize] = C12[i][j];
            C[i + newSize][j] = C21[i][j];
            C[i + newSize][j + newSize] = C22[i][j];
        }
    }
    
    // Free all allocated memory
    freeMatrix(A11, newSize); freeMatrix(A12, newSize);
    freeMatrix(A21, newSize); freeMatrix(A22, newSize);
    freeMatrix(B11, newSize); freeMatrix(B12, newSize);
    freeMatrix(B21, newSize); freeMatrix(B22, newSize);
    freeMatrix(C11, newSize); freeMatrix(C12, newSize);
    freeMatrix(C21, newSize); freeMatrix(C22, newSize);
    freeMatrix(M1, newSize); freeMatrix(M2, newSize);
    freeMatrix(M3, newSize); freeMatrix(M4, newSize);
    freeMatrix(M5, newSize); freeMatrix(M6, newSize);
    freeMatrix(M7, newSize);
    freeMatrix(temp1, newSize); freeMatrix(temp2, newSize);
}

// Function to generate random matrix
void generateRandomMatrix(int** mat, int n, int maxValue) {
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            mat[i][j] = rand() % maxValue;
        }
    }
}

// Function to get time in seconds
double getTime() {
    return (double)clock() / CLOCKS_PER_SEC;
}

void appendResultsToJSON(const char* filename, int n, double timeStrassen, double timeStandard) {
    FILE* file = fopen(filename, "r+"); 
    if (!file) {
        // File doesn't exist, create it
        file = fopen(filename, "w");
        if (!file) {
            printf("Error: Could not open file %s\n", filename);
            return;
        }
        fprintf(file, "[\n"); // start JSON array
    } else {
        // Move to end and check size
        fseek(file, 0, SEEK_END);
        long fileSize = ftell(file);

        if (fileSize > 2) {
            // Go back before the closing bracket ']'
            fseek(file, -2, SEEK_END);
            fprintf(file, ",\n");
        }
    }

    // Write JSON object
    fprintf(file, "  {\n");
    fprintf(file, "    \"matrix_size\": %d,\n", n);
    fprintf(file, "    \"strassen_time_seconds\": %.6f,\n", timeStrassen);
    fprintf(file, "    \"standard_time_seconds\": %.6f\n", timeStandard);
    fprintf(file, "  }\n");
    fprintf(file, "]\n");
    fclose(file);
}

int main() {
    srand(time(NULL)); 
    
    int maxValue = 100; 
    
    for (int power = 1; power <= 14; power++) {
        int size = 1 << power; 
        printf("==========================================\n");
        printf("** Matrix size: %d x %d (2^%d)\n", size, size, power);
        printf("==========================================\n");
        // Allocate matrices
        int** A = allocateMatrix(size);
        int** B = allocateMatrix(size);
        int** C = allocateMatrix(size);
        
        // Generate random matrices
        generateRandomMatrix(A, size, maxValue);
        generateRandomMatrix(B, size, maxValue);
        
        // Measure time for Strassen's algorithm
        double startStrassen = getTime();
        strassenMultiply(A, B, C, size);
        double endStrassen = getTime();
        double timeStrassen = endStrassen - startStrassen;

        // Measure time for Standard's algorithm
        double timeStandard = -1.0;
        int** D = allocateMatrix(size);
        double startStandard = getTime();
        standardMultiply(A, B, D, size);
        double endStandard = getTime();
        timeStandard = endStandard - startStandard;
        
        printf("Strassen's algorithm time: %.6f seconds\n", timeStrassen);
        printf("Standard algorithm time: %.6f seconds\n\n", timeStandard);
        
        freeMatrix(D, size);
        
        appendResultsToJSON("matrix_results_history.json", size, timeStrassen, timeStandard);
        
        // Free memory
        freeMatrix(A, size);
        freeMatrix(B, size);
        freeMatrix(C, size);
        
    }
    
    printf("\n==========================================\n");
    printf("All tests completed!\n");
    printf("==========================================\n");

    return 0;
}