import json
from collections import defaultdict
from heapq import heappush, heappop

def build_graph():
    graph = defaultdict(lambda: defaultdict(lambda: float('inf')))

    # Read times.jsonl
    with open('./lines.jsonl', 'r') as f:
        for line in f:
            data = json.loads(line)
            key1 = (data['from_id'], data['from_line'])
            key2 = (data['to_id'], data['to_line'])
            graph[key1][key2] = data['time']

    return graph

def dijkstra(graph, start):
    distances = defaultdict(lambda: float('inf'))
    distances[start] = 0
    pq = [(0, start)]

    while pq:
        current_distance, current = heappop(pq)

        if current_distance > distances[current]:
            continue

        for neighbor, weight in graph[current].items():
            distance = current_distance + weight

            if distance < distances[neighbor]:
                distances[neighbor] = distance
                heappush(pq, (distance, neighbor))

    return distances

def is_ground_node(node):
    return node[1] == "GROUND"

def main():
    # Build the graph
    graph = build_graph()

    # Get all unique nodes
    nodes = set()
    for node in graph:
        nodes.add(node)
        for neighbor in graph[node]:
            nodes.add(neighbor)

    # Calculate shortest paths between all pairs
    with open('./shortest_paths.jsonl', 'w') as f:
        for start in nodes:
            if not is_ground_node(start):
                continue
            distances = dijkstra(graph, start)
            for end in nodes:
                if distances[end] != float('inf') and start != end and is_ground_node(end):
                    result = {
                        'from_station': start[0],
                        'to_station': end[0],
                        'time': int(distances[end])
                    }
                    f.write(json.dumps(result) + '\n')

if __name__ == "__main__":
    main()
