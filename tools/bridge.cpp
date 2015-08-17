// http://stackoverflow.com/questions/2275550/change-stack-size-for-a-c-application-in-linux-during-compilation-with-gnu-com
// http://www.geeksforgeeks.org/bridge-in-a-graph/
// A C++ program to find bridges in a given undirected graph
#include <iostream>
#include <sys/resource.h>
#include <fstream>
#include <cstdlib>
#include <list>
#define NIL -1
using namespace std;

unsigned long bridge_count = 0;
 
// A class that represents an undirected graph
class Graph
{
    int V;    // No. of vertices
    list<int> *adj;    // A dynamic array of adjacency lists
    void bridgeUtil(int v, bool visited[], int disc[], int low[], int parent[]);
public:
    Graph(int V);   // Constructor
    void addEdge(int v, int w);   // function to add an edge to graph
    void bridge();    // prints all bridges
};
 
Graph::Graph(int V)
{
    this->V = V;
    adj = new list<int>[V];
}
 
void Graph::addEdge(int v, int w)
{
		if (v >= Graph::V || w >= Graph::V) {
			cout << "vertex id exceeds max id\n";
		}
    adj[v].push_back(w);
    adj[w].push_back(v);  // Note: the graph is undirected
}
 
// A recursive function that finds and prints bridges using DFS traversal
// u --> The vertex to be visited next
// visited[] --> keeps tract of visited vertices
// disc[] --> Stores discovery times of visited vertices
// parent[] --> Stores parent vertices in DFS tree
void Graph::bridgeUtil(int u, bool visited[], int disc[], 
                                       int low[], int parent[])
{
    // A static variable is used for simplicity, we can avoid use of static
    // variable by passing a pointer.
    static int time = 0;
 
    // Mark the current node as visited
    visited[u] = true;
 
    // Initialize discovery time and low value
    disc[u] = low[u] = ++time;
 
    // Go through all vertices aadjacent to this
    list<int>::iterator i;
    for (i = adj[u].begin(); i != adj[u].end(); ++i)
    {
        int v = *i;  // v is current adjacent of u
 
        // If v is not visited yet, then recur for it
        if (!visited[v])
        {
            parent[v] = u;
            bridgeUtil(v, visited, disc, low, parent);
 
            // Check if the subtree rooted with v has a connection to
            // one of the ancestors of u
            low[u]  = min(low[u], low[v]);
 
            // If the lowest vertex reachable from subtree under v is 
            // below u in DFS tree, then u-v is a bridge
            if (low[v] > disc[u])
							++bridge_count;
//							cout << ++(::bridge_count) << endl;
//              cout << u <<" " << v << endl;
        }
 
        // Update low value of u for parent function calls.
        else if (v != parent[u])
            low[u]  = min(low[u], disc[v]);
    }
}
 
// DFS based function to find all bridges. It uses recursive function bridgeUtil()
void Graph::bridge()
{
		bridge_count = 0;
    // Mark all the vertices as not visited
    bool *visited = new bool[V];
    int *disc = new int[V];
    int *low = new int[V];
    int *parent = new int[V];
 
    // Initialize parent and visited arrays
    for (int i = 0; i < V; i++)
    {
        parent[i] = NIL;
        visited[i] = false;
    }
 
    // Call the recursive helper function to find Bridges
    // in DFS tree rooted with vertex 'i'
    for (int i = 0; i < V; i++)
        if (visited[i] == false)
            bridgeUtil(i, visited, disc, low, parent);
}
 
// Driver program to test above function
int main(int argc, char** argv)
{
	if (argc != 2) {
		cout << "./bridge input_file" << endl;
		return -1;
	}

	const rlim_t stack_size = 512 * 1024 * 1024;
	struct rlimit rl;
	int result;

	result = getrlimit(RLIMIT_STACK, &rl);
	if (result != 0) {
		cout << "can not get stack limit\n";
		return -2;
	}

	if(rl.rlim_cur < stack_size) {
		rl.rlim_cur = stack_size;
		result = setrlimit(RLIMIT_STACK, &rl);
		if (result != 0) {
			cout << "can not set stack limit\n";
			return -2;
		} else {
			cout << "set stack to " << stack_size << "\n";
		}
	}

	ifstream edgelist;
	edgelist.open(argv[1], ios::in);

	int max_id = 0;
	int tmp_id = 0;
	while(!edgelist.eof()) {
		edgelist >> tmp_id;
		max_id = max_id > tmp_id ? max_id : tmp_id;
	}

	max_id = max_id + 10;
	
	cout << "max id " << max_id << endl;

	edgelist.close();

	edgelist.open(argv[1], ios::in);

	Graph g(max_id);

	int src, dst;
	while(!edgelist.eof()) {
		edgelist >> src >> dst;
		g.addEdge(src, dst);
	}

	cout << "graph loaded\n" << endl;
  
	g.bridge();

	cout << "bridges " << bridge_count << endl;
 
  return 0;
}
