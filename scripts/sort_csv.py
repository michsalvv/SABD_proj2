import pandas as pd



unsorted_csv = "../data/2022-05_bmp180.csv"
sorted_csv = "../data/sorted.csv"


  
# assign dataset
csvData = pd.read_csv(unsorted_csv,sep=";")
print (csvData)

  
# sort data frame
csvData.sort_values(csvData.columns[5], 
                    axis=0,
                    ascending=[True], 
                    inplace=True)
        
csvData.to_csv(sorted_csv,sep=";",index=False)
print (csvData)
