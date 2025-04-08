import json
import pathlib
import re
from argparse import ArgumentParser

import pandas as pd
from dash import Dash, dcc, html, callback, Output, Input
import plotly.express as px


parser = ArgumentParser(description="Plot execution time by item count, delay, and configuration.")
parser.add_argument(
    "--dir",
    type=str,
    default="first",
    help="Directory containing the JSON files with the results."
)
args = parser.parse_args()

res_dir = pathlib.Path(args.dir)

rows = []
for file_path in res_dir.glob("*.json"):
    print(f"Processing {file_path}")

    haystack = file_path.stem
    match = re.match(r"(\d+ms)-(\d+\wbit)", haystack)
    if not match: raise ValueError(f"File name {haystack} does not match expected pattern.")
    delay = str(match.group(1))
    bandwidth = str(match.group(2))
    
    with open(file_path, 'r') as f:
        results = json.load(f)["results"]
        for entry in results:
            data = {}
            data["config"] = entry["parameters"]["type"]
            data["size"] = str(entry["parameters"]["size"])
            data["execution time"] = entry["mean"]
            data["stddev"] = entry["stddev"]
            data["min"] = entry["min"]
            data["max"] = entry["max"]
            data["bandwidth"] = bandwidth
            data["delay"] = delay
            rows.append(data)
df = pd.DataFrame(rows)

app = Dash()
app.layout = html.Div([
    html.H1(children='Flowunits vs Renoir', style={'textAlign':'center'}),
    html.Div([
        html.Label("Select Bandwidth:"),
        dcc.Dropdown(df.bandwidth.unique(), df.bandwidth.unique()[0], id='bandwidth-selection'),
    ], style={'padding': 10}),
    html.Div([
        html.Label("Select Latency:"),
        dcc.Dropdown(df.delay.unique(), df.delay.unique()[0], id='latency-selection'),
    ], style={'padding': 10}),
    dcc.Graph(id='graph-content'),
], style={'width': '80%', 'margin': 'auto'})

@callback(
    Output('graph-content', 'figure'),
    Input('bandwidth-selection', 'value'),
    Input('latency-selection', 'value'),
)
def update_bandwidth(bandwidth, latency):
    filtered_df = df[
        (df['bandwidth'] == bandwidth) &
        (df['delay'] == latency)
    ]
    fig = px.bar(filtered_df, x="size", y="execution time", color="config", error_y="stddev", log_y=True, barmode="group")
    return fig


if __name__ == '__main__':
    app.run(debug=True)

