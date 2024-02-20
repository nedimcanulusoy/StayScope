import streamlit as st
import asyncio, json, re
from ollama import AsyncClient
from cleantext import clean
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.config import LLM_MODEL

class AsyncTextGenerator:
    def __init__(self):
        pass  # No initialization needed if we're directly displaying the text

    async def generate_text_stream(self, data):
        prompt_template = """This is your system prompt and you must do exactly what is written here. Based on the following data, please generate a brief report outlining notable trends, 
        comparing different scenarios, and offering actionable suggestions where appropriate in hospitality industry. Keep the report under two paragraphs in length. 
        Lastly, do NOT mention JSON or do NOT say like "This is a JSON response", "This is a JSON response from an Elasticsearch query" or "The response contains" in the report."""

        # Serialize and format the data
        serialized_data = json.dumps(data, indent=2)
        formatted_data = '"""' + serialized_data + '"""'

        prompt = f"{prompt_template}\n\n{formatted_data}"

        message = {'role': 'user', 'content': prompt}
        content = ''
        async for part in await AsyncClient().chat(model=LLM_MODEL, messages=[message], stream=True):
            content += part['message']['content'] + ''
        
        cleaned_content = clean(content, no_line_breaks=True, fix_unicode=True, to_ascii=True, lower=False)


        st.expander('Generated Report', expanded=True).write(cleaned_content) #cleaned_content)

    def run_async_in_thread(self, data):
        async def run():
            await self.generate_text_stream(data)

        # Start the async function in a new event loop running in a separate thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run())

    def generate_report_on_button_click(self, data, key='default'):
        if st.button('Generate Report', key=key):
            with st.spinner('Generating report...'):
                asyncio.run(self.generate_text_stream(data))