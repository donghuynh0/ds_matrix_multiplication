#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <string.h>

// Function to allocate a matrix
int** allocateMatrix(int n) {
    int** mat = (int**)malloc(n * sizeof(int*));
    for (int i = 0; i < n; i++) {
        mat[i] = (int*)malloc(n * sizeof(int));
    }
    return mat;
}

// Function to allocate shared memory matrix
int** allocateSharedMatrix(int n) {
    int** mat = (int**)mmap(NULL, n * sizeof(int*), 
                            PROT_READ | PROT_WRITE, 
                            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    for (int i = 0; i < n; i++) {
        mat[i] = (int*)mmap(NULL, n * sizeof(int), 
                            PROT_READ | PROT_WRITE, 
                            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
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

// Function to free shared memory matrix
void freeSharedMatrix(int** mat, int n) {
    for (int i = 0; i < n; i++) {
        munmap(mat[i], n * sizeof(int));
    }
    munmap(mat, n * sizeof(int*));
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

// Sequential Strassen (used for smaller subproblems)
void strassenMultiplySequential(int** A, int** B, int** C, int n) {
    if (n <= 64) {
        standardMultiply(A, B, C, n);
        return;
    }
    
    int newSize = n / 2;
    
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
    
    addMatrix(A11, A22, temp1, newSize);
    addMatrix(B11, B22, temp2, newSize);
    strassenMultiplySequential(temp1, temp2, M1, newSize);
    
    addMatrix(A21, A22, temp1, newSize);
    strassenMultiplySequential(temp1, B11, M2, newSize);
    
    subtractMatrix(B12, B22, temp2, newSize);
    strassenMultiplySequential(A11, temp2, M3, newSize);
    
    subtractMatrix(B21, B11, temp2, newSize);
    strassenMultiplySequential(A22, temp2, M4, newSize);
    
    addMatrix(A11, A12, temp1, newSize);
    strassenMultiplySequential(temp1, B22, M5, newSize);
    
    subtractMatrix(A21, A11, temp1, newSize);
    addMatrix(B11, B12, temp2, newSize);
    strassenMultiplySequential(temp1, temp2, M6, newSize);
    
    subtractMatrix(A12, A22, temp1, newSize);
    addMatrix(B21, B22, temp2, newSize);
    strassenMultiplySequential(temp1, temp2, M7, newSize);
    
    addMatrix(M1, M4, temp1, newSize);
    subtractMatrix(temp1, M5, temp2, newSize);
    addMatrix(temp2, M7, C11, newSize);
    
    addMatrix(M3, M5, C12, newSize);
    addMatrix(M2, M4, C21, newSize);
    
    subtractMatrix(M1, M2, temp1, newSize);
    addMatrix(temp1, M3, temp2, newSize);
    addMatrix(temp2, M6, C22, newSize);
    
    for (int i = 0; i < newSize; i++) {
        for (int j = 0; j < newSize; j++) {
            C[i][j] = C11[i][j];
            C[i][j + newSize] = C12[i][j];
            C[i + newSize][j] = C21[i][j];
            C[i + newSize][j + newSize] = C22[i][j];
        }
    }
    
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

// Parallel Strassen using fork()
void strassenMultiplyParallel(int** A, int** B, int** C, int n) {
    // Use sequential for small matrices or deep recursion
    if (n <= 256) {
        strassenMultiplySequential(A, B, C, n);
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
    
    // Use shared memory for M matrices so child processes can write to them
    int** M1 = allocateSharedMatrix(newSize);
    int** M2 = allocateSharedMatrix(newSize);
    int** M3 = allocateSharedMatrix(newSize);
    int** M4 = allocateSharedMatrix(newSize);
    int** M5 = allocateSharedMatrix(newSize);
    int** M6 = allocateSharedMatrix(newSize);
    int** M7 = allocateSharedMatrix(newSize);
    
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
    
    pid_t pids[7];
    
    // M1 = (A11 + A22) * (B11 + B22)
    pids[0] = fork();
    if (pids[0] == 0) {
        addMatrix(A11, A22, temp1, newSize);
        addMatrix(B11, B22, temp2, newSize);
        strassenMultiplySequential(temp1, temp2, M1, newSize);
        exit(0);
    }
    
    // M2 = (A21 + A22) * B11
    pids[1] = fork();
    if (pids[1] == 0) {
        int** t1 = allocateMatrix(newSize);
        addMatrix(A21, A22, t1, newSize);
        strassenMultiplySequential(t1, B11, M2, newSize);
        freeMatrix(t1, newSize);
        exit(0);
    }
    
    // M3 = A11 * (B12 - B22)
    pids[2] = fork();
    if (pids[2] == 0) {
        int** t2 = allocateMatrix(newSize);
        subtractMatrix(B12, B22, t2, newSize);
        strassenMultiplySequential(A11, t2, M3, newSize);
        freeMatrix(t2, newSize);
        exit(0);
    }
    
    // M4 = A22 * (B21 - B11)
    pids[3] = fork();
    if (pids[3] == 0) {
        int** t2 = allocateMatrix(newSize);
        subtractMatrix(B21, B11, t2, newSize);
        strassenMultiplySequential(A22, t2, M4, newSize);
        freeMatrix(t2, newSize);
        exit(0);
    }
    
    // M5 = (A11 + A12) * B22
    pids[4] = fork();
    if (pids[4] == 0) {
        int** t1 = allocateMatrix(newSize);
        addMatrix(A11, A12, t1, newSize);
        strassenMultiplySequential(t1, B22, M5, newSize);
        freeMatrix(t1, newSize);
        exit(0);
    }
    
    // M6 = (A21 - A11) * (B11 + B12)
    pids[5] = fork();
    if (pids[5] == 0) {
        int** t1 = allocateMatrix(newSize);
        int** t2 = allocateMatrix(newSize);
        subtractMatrix(A21, A11, t1, newSize);
        addMatrix(B11, B12, t2, newSize);
        strassenMultiplySequential(t1, t2, M6, newSize);
        freeMatrix(t1, newSize);
        freeMatrix(t2, newSize);
        exit(0);
    }
    
    // M7 = (A12 - A22) * (B21 + B22)
    pids[6] = fork();
    if (pids[6] == 0) {
        int** t1 = allocateMatrix(newSize);
        int** t2 = allocateMatrix(newSize);
        subtractMatrix(A12, A22, t1, newSize);
        addMatrix(B21, B22, t2, newSize);
        strassenMultiplySequential(t1, t2, M7, newSize);
        freeMatrix(t1, newSize);
        freeMatrix(t2, newSize);
        exit(0);
    }
    
    // Wait for all child processes
    for (int i = 0; i < 7; i++) {
        waitpid(pids[i], NULL, 0);
    }
    
    // Calculate C quadrants
    addMatrix(M1, M4, temp1, newSize);
    subtractMatrix(temp1, M5, temp2, newSize);
    addMatrix(temp2, M7, C11, newSize);
    
    addMatrix(M3, M5, C12, newSize);
    addMatrix(M2, M4, C21, newSize);
    
    subtractMatrix(M1, M2, temp1, newSize);
    addMatrix(temp1, M3, temp2, newSize);
    addMatrix(temp2, M6, C22, newSize);
    
    // Combine quadrants
    for (int i = 0; i < newSize; i++) {
        for (int j = 0; j < newSize; j++) {
            C[i][j] = C11[i][j];
            C[i][j + newSize] = C12[i][j];
            C[i + newSize][j] = C21[i][j];
            C[i + newSize][j + newSize] = C22[i][j];
        }
    }
    
    // Free memory
    freeMatrix(A11, newSize); freeMatrix(A12, newSize);
    freeMatrix(A21, newSize); freeMatrix(A22, newSize);
    freeMatrix(B11, newSize); freeMatrix(B12, newSize);
    freeMatrix(B21, newSize); freeMatrix(B22, newSize);
    freeMatrix(C11, newSize); freeMatrix(C12, newSize);
    freeMatrix(C21, newSize); freeMatrix(C22, newSize);
    freeSharedMatrix(M1, newSize); freeSharedMatrix(M2, newSize);
    freeSharedMatrix(M3, newSize); freeSharedMatrix(M4, newSize);
    freeSharedMatrix(M5, newSize); freeSharedMatrix(M6, newSize);
    freeSharedMatrix(M7, newSize);
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

void appendResultsToJSON(const char* filename, int n, double timeParallel) {
    FILE* file = fopen(filename, "r+"); 
    if (!file) {
        file = fopen(filename, "w");
        if (!file) {
            printf("Error: Could not open file %s\n", filename);
            return;
        }
        fprintf(file, "[\n");
    } else {
        fseek(file, 0, SEEK_END);
        long fileSize = ftell(file);

        if (fileSize > 2) {
            fseek(file, -2, SEEK_END);
            fprintf(file, ",\n");
        }
    }

    fprintf(file, "  {\n");
    fprintf(file, "    \"matrix_size\": %d,\n", n);
    fprintf(file, "    \"parallel_strassen_time_seconds\": %.6f\n", timeParallel);
    fprintf(file, "  }\n");
    fprintf(file, "]\n");
    fclose(file);
}

int main() {
    srand(time(NULL)); 
    
    int maxValue = 100; 
    
    for (int power = 2; power <= 14; power++) {
        int size = 1 << power; 
        printf("==========================================\n");
        printf("** Matrix size: %d x %d (2^%d)\n", size, size, power);
        printf("==========================================\n");
        
        int** A = allocateMatrix(size);
        int** B = allocateMatrix(size);
        int** C = allocateMatrix(size);
        
        generateRandomMatrix(A, size, maxValue);
        generateRandomMatrix(B, size, maxValue);
        
        // Measure parallel Strassen
        double startParallel = getTime();
        strassenMultiplyParallel(A, B, C, size);
        double endParallel = getTime();
        double timeParallel = endParallel - startParallel;
        
        printf("Parallel Strassen time: %.6f seconds\n\n", timeParallel);
        
        appendResultsToJSON("matrix_results_parallel.json", size, timeParallel);
        
        freeMatrix(A, size);
        freeMatrix(B, size);
        freeMatrix(C, size);
    }
    
    printf("\n==========================================\n");
    printf("All tests completed!\n");
    printf("==========================================\n");

    return 0;
}