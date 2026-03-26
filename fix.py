import pandas as pd

with open('data.csv', 'r') as f:
    content = f.read()

content = content.replace('"', '')

with open('data_fixed.csv', 'w') as f:
    f.write(content)

df = pd.read_csv('data_fixed.csv')
print(df.columns.tolist())
print("Done!")