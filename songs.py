import sys
import csv
from multiprocessing import Process
from collections import defaultdict

N_MAP = int(sys.argv[1])
N_REDUCE = int(sys.argv[2])

def mapper(input_file, output_file):
    with open(input_file, 'r') as f, open(output_file, 'w') as out:
        reader = csv.reader(f)
        for row in reader:
            try:
                artist = row[1]
                duration = float(row[2])
                out.write(f"{artist}\t{duration}\n")
            except:
                continue

def reducer(input_file, output_file):
    max_duration = defaultdict(float)
    with open(input_file, 'r') as f:
        for line in f:
            artist, dur = line.strip().split('\t')
            max_duration[artist] = max(max_duration[artist], float(dur))
    with open(output_file, 'w') as out:
        for artist, dur in max_duration.items():
            out.write(f"{artist},{dur}\n")
if __name__ == '__main__':
# Step 1: Run Mappers
    map_processes = []
    for i in range(N_MAP):
        part_number = f"{i+1:02d}"  # match music_part.01 to music_part.20
        in_file = f"music_part{part_number}.csv"
        out_file = f"map_output_{i:02d}.txt"
        p = Process(target=mapper, args=(in_file, out_file))
        p.start()
        map_processes.append(p)
    
    for p in map_processes:
        p.join()
    
    # Step 2: Shuffle
    buckets = [defaultdict(list) for _ in range(N_REDUCE)]
    
    for i in range(N_MAP):
        with open(f"map_output_{i:02d}.txt", 'r') as f:
            for line in f:
                artist, duration = line.strip().split('\t')
                bucket_id = hash(artist) % N_REDUCE
                buckets[bucket_id][artist].append(float(duration))
    
    for i in range(N_REDUCE):
        with open(f"shuffle_{i:02d}.txt", 'w') as f:
            for artist, durations in buckets[i].items():
                for d in durations:
                    f.write(f"{artist}\t{d}\n")
    
    # Step 3: Run Reducers
    reduce_processes = []
    for i in range(N_REDUCE):
        in_file = f"shuffle_{i:02d}.txt"
        out_file = f"reducer_output_{i:02d}.txt"
        p = Process(target=reducer, args=(in_file, out_file))
        p.start()
        reduce_processes.append(p)
    
    for p in reduce_processes:
        p.join()
    
    # Step 4: Merge output (optional)
    with open("final_results.csv", 'w') as fout:
        for i in range(N_REDUCE):
            with open(f"reducer_output_{i:02d}.txt", 'r') as f:
                fout.write(f.read())