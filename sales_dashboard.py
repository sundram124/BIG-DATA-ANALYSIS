import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st

# Load Data (streamlit caching to improve performance)
@st.cache_data
def load_data(file_path):
    data = pd.read_csv(file_path)
    
    # Print column names to debug
    print("Columns in the dataset:", data.columns)
    
    # Strip any extra spaces in column names
    data.columns = data.columns.str.strip()
    
    # Check if 'Sales' column exists
    if 'Sales' not in data.columns:
        st.error("Sales column not found in the data!")
        return None
    
    return data

# Basic Analysis
def analyze_data(data):
    total_sales = data['Sales'].sum()
    avg_sales = data['Sales'].mean()
    top_product = data.groupby('Product')['Sales'].sum().idxmax()
    top_region = data.groupby('Region')['Sales'].sum().idxmax()
    return total_sales, avg_sales, top_product, top_region

# Visualization
def visualize_data(data):
    st.subheader("Sales by Region")
    region_sales = data.groupby('Region')['Sales'].sum().reset_index()
    sns.barplot(x='Region', y='Sales', data=region_sales)
    plt.title("Total Sales by Region")
    st.pyplot(plt)

    st.subheader("Sales by Product")
    product_sales = data.groupby('Product')['Sales'].sum().reset_index()
    sns.barplot(x='Product', y='Sales', data=product_sales, palette="coolwarm")
    plt.title("Total Sales by Product")
    st.pyplot(plt)

# Streamlit App
def main():
    st.title("Sales Performance Dashboard")

    st.sidebar.header("Upload CSV")
    uploaded_file = st.sidebar.file_uploader("Upload your sales data (CSV)", type=["csv"])

    if uploaded_file:
        data = load_data(uploaded_file)
        
        # Check if data is None due to missing columns
        if data is None:
            return

        st.write("### Data Preview", data.head())

        total_sales, avg_sales, top_product, top_region = analyze_data(data)

        st.write("### Key Metrics")
        st.write(f"**Total Sales:** {total_sales}")
        st.write(f"**Average Sales:** {avg_sales:.2f}")
        st.write(f"**Top Product:** {top_product}")
        st.write(f"**Top Region:** {top_region}")

        visualize_data(data)
    else:
        st.write("Upload a CSV file to see analysis.")

if __name__ == "__main__":
    main()
