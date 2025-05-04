import streamlit as st
import joblib
import numpy as np

# Load the pre-trained model
model = joblib.load("iris_model.pkl")

# Define the mapping for iris species
species = {0: "setosa", 1: "versicolor", 2: "virginica"}

# App Title
st.title("Iris Species Classifier")
st.write("Enter the measurements of an Iris flower to predict its species.")

# Input widgets for user to enter measurements
sepal_length = st.number_input("Sepal Length (cm)", min_value=0.0, value=5.1)
sepal_width  = st.number_input("Sepal Width (cm)", min_value=0.0, value=3.5)
petal_length = st.number_input("Petal Length (cm)", min_value=0.0, value=1.4)
petal_width  = st.number_input("Petal Width (cm)", min_value=0.0, value=0.2)

if st.button("Predict"):
    # Prepare the input as a 2D array for prediction
    input_features = np.array([[sepal_length, sepal_width, petal_length, petal_width]])
    prediction = model.predict(input_features)
    st.success(f"The predicted Iris species is **{species[prediction[0]]}**.")


### TO-DO: ADD A BAR CHART WITH THE MEASUREMENTS ENTERED.
### Create a dataframe first with the input values. Then use streamlit's bar_chart function: https://docs.streamlit.io/develop/api-reference/charts/st.bar_chart

