
#corregir filas malformadas
import csv

INPUT = 'styles.csv'
OUTPUT = 'styles_fixed.csv'

with open(INPUT, newline='', encoding='utf-8') as fin, open(OUTPUT, 'w', newline='', encoding='utf-8') as fout:

    reader = csv.reader(fin)
    writer = csv.writer(fout, quoting=csv.QUOTE_MINIMAL)
    
    header = next(reader)
    writer.writerow(header)
    expected = len(header)
    
    for lineno, row in enumerate(reader, start=2):
        if len(row) == expected:
            writer.writerow(row)
        elif len(row) > expected:
            fixed = row[:expected-1] + [','.join(row[expected-1:]).strip()]
            writer.writerow(fixed)
        else:
            writer.writerow(row)

print(f"✅ styles_fixed.csv generado con {OUTPUT}")


# juntar los dos csv
import pandas as pd

images = pd.read_csv('images.csv')
styles = pd.read_csv('styles_fixed.csv')

images['id'] = images['filename'].str.replace(r'\.\w+$', '', regex=True).astype(int)


merged = pd.merge(images, styles, on='id', how='inner')

cols = ['id', 'filename', 'link'] + [c for c in merged.columns if c not in ('id','filename','link')]
merged = merged[cols]


merged.to_csv('data.csv', index=False, encoding='utf-8')
print(f"Generado data.csv con {len(merged)} filas.")



#generar data (id, full text)

import pandas as pd

df = pd.read_csv('data.csv')

exclude = {'filename', 'link'}
text_cols = [c for c in df.columns if c not in ('id', *exclude)]

df['text'] = (
    df[text_cols]
    .fillna('')
    .astype(str)
    .agg(' '.join, axis=1)
)


out = df[['id', 'text']]


out.to_csv('id_text.csv', index=False, encoding='utf-8')

print(f"✅ id_text.csv generado con {len(out)} filas.")