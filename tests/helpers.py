def areDataframesEquals(df1, df2):
    return df1.subtract(df2).count() == 0