# NLP-Exploration-in-Bioinformatics
An Natural Language Processing (NLP) exploration in Bioinformatic literatures in 2016 autumn semester. To try to answer the question "which **gene** associates with which **disease** through which **bioevent**", [Turku Event Extraction System (TEES)](http://bionlp.utu.fi/eventextractionsoftware.html) was used to extract triples of **_Gene Name_**, **_Disease Name_** and **_Bioevent_**. Then we could retrieve the derived database by any of the genes, diseases or bioevents, and display the search result.

## Bioevents
- Gene_expression 表达
- Regulation (Positive_regulation or Negative_regulation) 调控 (正或负)
- Binding 绑定
- Localization 定位
- Transcription 转录
- Phosphorylation 磷酸化

## Procedure 
### Step 1. [S1_ParseTextToSQL.py](/S1_ParseTextToSQL.py)
Extract **PubMed_ID**, **Title**, and **Abstract** into a **SQLite** database from raw CSV file.

As for how to download abstracts from PubMed, [an previous exercise](https://github.com/az7jh2/Download-PubMed-Abstracts) would be a good illustration.

### Step 2. [S2_PrepareForNER.py](/S2_PrepareForNER.py)
Read abstracts from **SQLite** database, then transform them to **txt** files for **Named Entity Recognition (NER)** by [ABNER](http://pages.cs.wisc.edu/~bsettles/abner/).

### Step 3. [S3_UsingABNER](/S3_UsingABNER)
Using [ABNER](http://pages.cs.wisc.edu/~bsettles/abner/) to recognize named entities.

### Step 4. [S4_ParseNERResult.py](/S4_ParseNERResult.py)
The output of NER by [ABNER](http://pages.cs.wisc.edu/~bsettles/abner/) was *SGML* files with a total of **5** tags:
- PROTEIN
- DNA
- RNA
- CELL_TYPE
- CELL_LINE

What we need were abstracts which containing DNA/Gene information, and abstracts without "DNA" tag were filtered out. 

### Step 5. [S5_ParseHumanGene.py](/S5_ParseHumanGene.py)
Build a SQLite database to associate **_GeneID_** with **official name** of **_Ensembl gene_** for gene names nomlization.

### Step 6. [S6_NormGeneName.py](/S6_NormGeneName.py)
Filter out abstractes without official gene names.

### Step 7. [S7_PrepareForEventFinding.py](/S7_PrepareForEventFinding.py)
Prepare files for bioevents parse using [TEES](http://bionlp.utu.fi/eventextractionsoftware.html).

### Step 8. [S8_UsingTEES](/S8_UsingTEES)
Using [TEES](http://bionlp.utu.fi/eventextractionsoftware.html) to parse bioevents.

### Step 9. [S9_ParseEventResult.py](/S9_ParseEventResult.py)
Parse the output of [TEES](http://bionlp.utu.fi/eventextractionsoftware.html) and save related information into a SQLite database.

P.S.
- The names of genes in SQLite database were official gene names.
- *Positive_regulation* and *Negative_regulation* were merged as *Regulation*

### Step 10. [S10_RetrieveData.py](/S10_RetrieveData.py)
Retrieve data from built SQLite database by any of the genes, diseases or bioevents.

## Results
### Flowchart of NLP Procedures
![Flowchart](/images/Flowchart.png)

### An Search Example
Gene name: *TP53*
Bioevents: *Gene_expression* & *Regulation*
![Search Results](/images/SearchExample.jpg)
