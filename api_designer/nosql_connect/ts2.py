from collections import defaultdict

class Graph:
    def __init__(self, vertices):
        self.graph = defaultdict(list)
        self.V = vertices

    def add_edge(self, u, v):
        self.graph[u].append(v)

    def ts(self):
        in_degree = [0]*(self.V)
        for i in self.graph:
            for j in self.graph[i]:
                in_degree[j] += 1

        queue = []
        for i in range(self.V):
            if in_degree[i] == 0:
                queue.append(i)

        cnt = 0
        ts_order = []
        #if len(queue) == 0:
            #queue.append(self.V-1)
            #for i in self.graph:

        while queue:
            u = queue.pop()
            ts_order.append(u)

            for i in self.graph[u]:
                in_degree[i] -= 1
                if in_degree[i] == 0:
                    queue.append(i)

            cnt += 1

        if cnt == self.V:
            return ts_order
        else:
            for i in self.graph:
                if i not in ts_order:
                    ts_order.append(i)
            #print("ts_order..", ts_order)
            return ts_order

def get_ts_order(table_data):
   print("table_data", table_data)
   for i in range(len(table_data)):
      if table_data[i]["key"] in table_data[i]["dependencies"]:
         table_data[i]["dependencies"].remove(table_data[i]["key"])

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

   table_order = G.ts()
   #print("table_order1", table_order)

   for i, to in enumerate(table_order):
        #print(i)
        table_order[i] = table_invert_mapping[table_order[i]]

   #print("table_order2", table_order)

   return table_order

"""
table_data = [
   {
      "key":"production.categories",
      "dependencies":[
         
      ]
   },
   {
      "key":"production.brands",
      "dependencies":[
         
      ]
   },
   {
      "key":"production.products",
      "dependencies":[
         "production.brands",
         "production.categories"
      ]
   },
   {
      "key":"production.stocks",
      "dependencies":[
         "sales.stores",
         "production.products"
      ]
   },
   {
      "key":"sales.customers",
      "dependencies":[
         
      ]
   },
   {
      "key":"sales.stores",
      "dependencies":[
         
      ]
   },
   {
      "key":"sales.staffs",
      "dependencies":[
         "sales.stores",
         "sales.staffs"
      ]
   },
   {
      "key":"sales.orders",
      "dependencies":[
         "sales.stores",
         "sales.staffs",
         "sales.customers"
      ]
   },
   {
      "key":"sales.order_items",
      "dependencies":[
         "production.products",
         "sales.orders"
      ]
   }
]

print(get_ts_order(table_data))
"""