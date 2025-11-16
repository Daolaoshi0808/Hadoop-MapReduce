# Hadoop-MapReduce
This repository contains two MapReduce implementations using Python for large-scale data processing.

# Problem 1: Google Books N-grams Analysis

## Overview
This implementation analyzes Google Books n-gram data to compute the average number of volumes per year for words containing specific substrings: 'nu', 'chi', and 'haw'.

## Dataset
- **Source**: Google Books N-gram dataset
- **Format**: Two files containing 1-grams (single words) and 2-grams (word pairs)

### Data Format
**1-gram format**:
```
word \s+ year \s+ occurrences \s+ volumes \s+ ...
```

**2-gram format**:
```
word1 \s+ word2 \s+ year \s+ occurrences \s+ volumes \s+ ...
```

## Implementation Files
- `mapper.py` - Filters and emits records containing target substrings
- `reducer.py` - Calculates average volumes per (year, substring) pair

## MapReduce Pseudo Code

### Map Phase
```
FUNCTION mapper():
    DEFINE substrings = ['nu', 'chi', 'haw']
    
    FOR each line from stdin:
        PARSE line into fields
        
        // Handle 1-gram (4 fields)
        IF length(fields) == 4:
            word = fields[0]
            year = fields[1]
            volumes = fields[3]
            
            VALIDATE year is integer in range (0, 2022]
            IF not valid:
                SKIP this record
            
            FOR each substring in substrings:
                IF substring in word:
                    EMIT "{year},{substring}\t{volumes}"
        
        // Handle 2-gram (5 fields)
        ELSE IF length(fields) == 5:
            word1 = fields[0]
            word2 = fields[1]
            year = fields[2]
            volumes = fields[4]
            
            VALIDATE year is integer in range (0, 2022]
            IF not valid:
                SKIP this record
            
            FOR each substring in substrings:
                IF substring in word1:
                    EMIT "{year},{substring}\t{volumes}"
                IF substring in word2:
                    EMIT "{year},{substring}\t{volumes}"
```

**Key Points**:
- If a 2-gram contains the substring in both words, it's counted twice
- Each occurrence adds to both numerator (volumes × count) and denominator (count)
- Invalid years (non-integers or out of range) are filtered out

**Output Format**: `year,substring\tvolumes`

Example:
```
2000,nu	150
2000,chi	200
1998,haw	75
```

---

### Reduce Phase
```
FUNCTION reducer():
    current_key = None
    total_sum = 0
    total_count = 0
    
    FOR each line from stdin:
        PARSE line into key and value
        key = "year,substring"
        value = volumes (float)
        
        IF key == current_key:
            // Same key, accumulate
            total_sum += value
            total_count += 1
        ELSE:
            // New key encountered
            IF current_key is not None:
                // Output previous key's result
                average = total_sum / total_count
                EMIT "{current_key},{average}"
            
            // Reset for new key
            current_key = key
            total_sum = value
            total_count = 1
    
    // Don't forget last key
    IF current_key is not None:
        average = total_sum / total_count
        EMIT "{current_key},{average}"
```

**Input Format**: `year,substring\tvolumes` (sorted by key)

**Output Format**: `year,substring,average_volumes`

Example:
```
2000,nu,345.5
2010,nu,200.3
1998,chi,31.2
```

---

## Execution
```bash
cat input_file.txt | python mapper.py | sort | python reducer.py > output.txt
```

Or with Hadoop Streaming:
```bash
hadoop jar hadoop-streaming.jar \
  -input /path/to/input \
  -output /path/to/output \
  -mapper mapper.py \
  -reducer reducer.py
```

## Sample Results
The output shows the average number of volumes per year for each substring:

```
2000,nu,345
2010,nu,200
1998,chi,31
2005,haw,128
```

---

# Problem 2: Maximum Song Duration per Artist

## Overview
This implementation finds the maximum song duration for each artist in the Million Song Dataset using a custom MapReduce framework with parallel processing.

## Dataset
- **Source**: Subset of Million Song Database
- **Format**: CSV with columns: song title (column 1), artist name (column 3), duration (column 4)
- **Size**: Split into 20 parts for parallel processing

## Execution
```bash
python songs.py <NUM_MAPPERS> <NUM_REDUCERS>
```

Example:
```bash
python songs.py 20 5
```
- 20 mapper processes (one per data split)
- 5 reducer processes

## MapReduce Implementation Pseudo Code

### Map Phase
```
FUNCTION mapper(input_file, output_file):
    OPEN input_file for reading
    OPEN output_file for writing
    
    FOR each row in input_file:
        TRY:
            EXTRACT artist_name from column 3
            EXTRACT duration from column 4
            CONVERT duration to float
            
            WRITE to output_file: "artist_name\tduration\n"
        CATCH any errors:
            SKIP this row (malformed data)
    
    CLOSE files
```

**Input**: `music_part01.csv`, `music_part02.csv`, ..., `music_part20.csv`

**Output**: `map_output_00.txt`, `map_output_01.txt`, ..., `map_output_19.txt`

**Format**: `artist\tduration`

---

### Shuffle Phase
```
FUNCTION shuffle(num_mappers, num_reducers):
    CREATE num_reducers empty buckets (each bucket is a dictionary)
    
    // Read all mapper outputs
    FOR i = 0 to num_mappers - 1:
        OPEN map_output_i.txt
        
        FOR each line in file:
            PARSE line to get artist and duration
            
            // Hash-based partitioning
            bucket_id = hash(artist) % num_reducers
            
            // Group all records for same artist
            ADD duration to buckets[bucket_id][artist]
    
    // Write shuffled data to reducer inputs
    FOR i = 0 to num_reducers - 1:
        OPEN shuffle_i.txt for writing
        
        FOR each artist in buckets[i]:
            FOR each duration in buckets[i][artist]:
                WRITE "artist\tduration\n"
        
        CLOSE file
```

**Input**: All `map_output_*.txt` files

**Output**: `shuffle_00.txt`, `shuffle_01.txt`, ..., `shuffle_04.txt` (for 5 reducers)

**Partitioning**: Hash-based - ensures all records for the same artist go to the same reducer

---

### Reduce Phase
```
FUNCTION reducer(input_file, output_file):
    CREATE empty dictionary: max_duration
    
    OPEN input_file for reading
    
    FOR each line in file:
        PARSE line to get artist and duration
        CONVERT duration to float
        
        // Track maximum duration per artist
        IF artist NOT in max_duration:
            max_duration[artist] = duration
        ELSE:
            max_duration[artist] = MAX(max_duration[artist], duration)
    
    OPEN output_file for writing
    
    FOR each artist in max_duration:
        WRITE "artist,max_duration\n"
    
    CLOSE files
```

**Input**: `shuffle_00.txt`, `shuffle_01.txt`, ..., `shuffle_04.txt`

**Output**: `reducer_output_00.txt`, `reducer_output_01.txt`, ..., `reducer_output_04.txt`

**Format**: `artist,max_duration`

---

### Main Execution Flow
```
MAIN:
    READ command line arguments: NUM_MAPPERS, NUM_REDUCERS
    
    // STEP 1: Map Phase (Parallel)
    CREATE empty list: map_processes
    
    FOR i = 0 to NUM_MAPPERS - 1:
        input_file = "music_part{i}.csv"
        output_file = "map_output_{i}.txt"
        
        CREATE process: mapper(input_file, output_file)
        START process
        ADD process to map_processes
    
    WAIT for all map_processes to complete
    
    
    // STEP 2: Shuffle Phase (Sequential)
    CREATE NUM_REDUCERS buckets for grouping
    
    FOR each map output file:
        READ all (artist, duration) pairs
        PARTITION by hash(artist) % NUM_REDUCERS
        GROUP by artist within each partition
    
    WRITE shuffled data to shuffle files
    
    
    // STEP 3: Reduce Phase (Parallel)
    CREATE empty list: reduce_processes
    
    FOR i = 0 to NUM_REDUCERS - 1:
        input_file = "shuffle_{i}.txt"
        output_file = "reducer_output_{i}.txt"
        
        CREATE process: reducer(input_file, output_file)
        START process
        ADD process to reduce_processes
    
    WAIT for all reduce_processes to complete
    
    
    // STEP 4: Merge Final Results (Sequential)
    OPEN "final_results.csv" for writing
    
    FOR i = 0 to NUM_REDUCERS - 1:
        READ reducer_output_{i}.txt
        WRITE all content to final_results.csv
    
    CLOSE final_results.csv
```

---

## Key Design Decisions

### Parallel Processing
- **Map Phase**: Each mapper runs as a separate process using Python's `multiprocessing.Process`
- **Reduce Phase**: Each reducer runs as a separate process
- **Shuffle Phase**: Runs sequentially after all mappers complete

### Hash-Based Partitioning
- Uses `hash(artist) % NUM_REDUCERS` to determine which reducer handles each artist
- Ensures all songs by the same artist go to the same reducer
- Distributes load across reducers

### Data Flow
```
music_part01.csv ──┐
music_part02.csv ──┼──> [MAPPERS] ──> map_output_*.txt
    ...            │                         │
music_part20.csv ──┘                         │
                                             ├──> [SHUFFLE] ──> shuffle_*.txt
                                             │                         │
                                             │                         ├──> [REDUCERS] ──> reducer_output_*.txt
                                             │                         │
                                             └─────────────────────────┘
                                                                       │
                                                                       └──> final_results.csv
```

## Sample Results
See `sample_result2.csv` for example output showing artists and their maximum song durations.

Format: `artist,max_duration`

Example:
```
Alicia Keys,255.99955
Charlotte Gainsbourg,241.65832
Alternative TV,305.71057
```
