from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict

app = FastAPI()


from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    
)

# Define the schema for nodes and edges

class Edge(BaseModel):
    source: str
    target: str

class Pipeline(BaseModel):
    nodes: List[str]
    edges: List[Edge]

def is_dag(nodes: List[str], edges: List[Edge]) -> bool:
    from collections import defaultdict, deque
    
    # Build adjacency list and in-degree count
    adjacency_list = defaultdict(list)
    in_degree = defaultdict(int)  # Changed to defaultdict(int)
    
    # Initialize in_degree for all nodes (including those with no incoming edges)
    for node in nodes:
        in_degree[node] = 0
    
    # Build adjacency list and count in-degrees
    for edge in edges:
        # Validate that edge nodes exist in the nodes list
        if edge.source not in nodes or edge.target not in nodes:
            raise ValueError(f"Edge {edge.source} -> {edge.target} contains nodes not in nodes list")
            
        adjacency_list[edge.source].append(edge.target)
        in_degree[edge.target] += 1
    
    # Nodes with no incoming edges
    queue = deque([node for node in nodes if in_degree[node] == 0])
    visited_count = 0
    
    # Perform topological sort
    while queue:
        current = queue.popleft()
        visited_count += 1
        
        for neighbor in adjacency_list[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    # Graph is a DAG if we visited all nodes
    return visited_count == len(nodes)

@app.get('/')
def read_root():
    return {"Hello": "World"}

@app.post('/pipelines/parse')
def parse_pipeline(pipeline: Pipeline):
    nodes = pipeline.nodes
    edges = pipeline.edges

    num_nodes = len(nodes)
    num_edges = len(edges)
    is_dag_result = is_dag(nodes, edges)

    return {
        "num_nodes": num_nodes,
        "num_edges": num_edges,
        "is_dag": is_dag_result
    }
