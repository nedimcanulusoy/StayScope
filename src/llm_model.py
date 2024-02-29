import streamlit as st
import asyncio, json
from ollama import AsyncClient
import sys, time, random, string
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import LLM_MODEL, PROMPT_TEMPLATE

class AsyncTextGenerator:
    def __init__(self):
        self.llm_model = None

    async def generate_text_stream(self, data):
        # Serialize and format the data
        serialized_data = json.dumps(data, indent=2)
        formatted_data = '"""' + serialized_data + '"""'

        prompt = f"{PROMPT_TEMPLATE}\n\n{formatted_data}"

        message = {'role': 'user', 'content': prompt}
        content = """"""
        start_time = time.time()  # Start the timer
        async for part in await AsyncClient().chat(model=self.llm_model, messages=[message], stream=True):
            content += part['message']['content'] + ''
        end_time = time.time()  # Stop the timer

        content_clean = content.replace("```markdown", "").replace("```", "").replace("#","##").replace("##","###")

        report_generation_time = end_time - start_time
        with st.expander(f'Generated Report [Took: {round(report_generation_time, 2)} sec]', expanded=True):
            st.markdown(content_clean)

    def set_model(self, model_choice):
        self.llm_model = model_choice

                
    def run_async_in_thread(self, data):
        async def run():
            await self.generate_text_stream(data)

        # Start the async function in a new event loop running in a separate thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run())
        
    def generate_report_on_button_click(self, data, key='default'):
        # Use a selectbox for choosing a model
        chosen_model = st.selectbox("Choose the LLM model", options=LLM_MODEL, index=0, key=f"model_choice_{key}")
        self.set_model(chosen_model)

        if st.button('Generate Report', key=key):
            with st.spinner('Generating report...'):
                self.run_async_in_thread(data)  # Use the method that runs async in a separate thread
