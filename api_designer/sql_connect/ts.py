from collections import defaultdict

class Graph:
	def __init__(self, vertices):
		self.V = vertices
		self.graph = defaultdict(list)
		self.visited = [False]*(self.V)
		self.stack = []

	def add_edge(self, u, v):
		self.graph[u].append(v)

	def dfs(self, v, order = []):
		self.visited[v] = True
		order.append(v)
		for i in self.graph[v]:
			if not self.visited[i]:
				self.dfs(i, order)
		return order

	def fill_order(self, v):
		self.visited[v] = True
		for i in self.graph[v]:
			if not self.visited[i]:
				self.fill_order(i)
		self.stack.append(v)

	def get_transpose(self):
		g = Graph(self.V)
		for i in self.graph:
			for j in self.graph[i]:
				g.add_edge(j, i)
		return g

	def scc(self):
            s_components = []
            for i in range(self.V):
                if not self.visited[i]:
                    self.fill_order(i)

            self.visited = [False]*(self.V)
            gr = self.get_transpose()
            while self.stack:
                i = self.stack.pop()
                if not self.visited[i]:
                    ret = self.dfs(i, order = [])
                    s_components.append(ret)

            return s_components

def get_ts_order(table_data):
    ntables = len(table_data)
    G = Graph(ntables)

    start = 0
    table_id = {}

    for td in table_data:
        table_id[td['key']] = start
        start += 1
    table_invert_mapping = {v: k for k, v in table_id.items()}

    for td in table_data:
        for dep in td["dependencies"]:
            u = table_id[dep]
            v = table_id[td['key']]
            G.add_edge(u, v)

    table_order = G.scc()

    for i, _ in enumerate(table_order):
        for j, _ in enumerate(table_order[i]):
            table_order[i][j] = table_invert_mapping[table_order[i][j]]

    return table_order