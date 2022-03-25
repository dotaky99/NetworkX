# import
from jaal import Jaal
from jaal.datasets import load_got
# load the data
edge_df, node_df = load_got()
# init Jaal and run server

# edge_df.loc[:, 'Title'] = edge_df.loc[:, 'behavior'].astype(str)

Jaal(edge_df, node_df).plot()
# Jaal(edge_df, node_df).plot(directed=True)
# Jaal(edge_df, node_df).plot(vis_opts={'height': '600px', # change height
#                                       'interaction':{'hover': True}, # turn on-off the hover
#                                       'physics':{'stabilization':{'iterations': 100}}}) # define the convergence iteration of network