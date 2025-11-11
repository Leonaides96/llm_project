from openai import OpenAI
import gradio as gr
import os
from dotenv import load_dotenv

print(f"env_loaded: {load_dotenv()}")

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def chat_with_gpt(message, history=[]):
    messages = [{"role": "system", "content": "You are a friendly AI assistant."}]
    for i, text in enumerate(history):
        role = "user" if i % 2 == 0 else "assistant"
        messages.append({"role": role, "content": text})
    messages.append({"role": "user", "content": message})

    response = client.chat.completions.create(
        model="gpt-4o-mini",  # you can use gpt-4o, gpt-4-turbo, or gpt-3.5-turbo
        messages=messages,
    )

    reply = response.choices[0].message.content
    history.append(message)
    history.append(reply)
    return reply, history

gr.Interface(
    fn=chat_with_gpt,
    inputs="text",
    outputs="text",
    title="💬 ChatGPT-powered Local Chatbot",
    description="A chatbot that uses OpenAI's ChatGPT model."
).launch()
