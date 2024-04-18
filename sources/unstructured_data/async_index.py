from typing import Any, List, Optional

from langchain.base_language import BaseLanguageModel
from langchain.chains.retrieval_qa.base import RetrievalQA
from langchain.indexes.vectorstore import (
    VectorstoreIndexCreator,
    VectorStoreIndexWrapper,
)
from langchain.llms.openai import OpenAI
from langchain.schema import Document


class AVectorStoreIndexWrapper(VectorStoreIndexWrapper):
    """Async wrapper around a vectorstore for easy access."""

    def __init__(
        self,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

    async def aquery(
        self, question: str, llm: Optional[BaseLanguageModel] = None, **kwargs: Any
    ) -> str:
        """Query the vectorstore."""
        llm = llm or OpenAI(temperature=0)  # type: ignore[call-arg]
        chain = RetrievalQA.from_chain_type(
            llm, retriever=self.vectorstore.as_retriever(), **kwargs
        )
        return await chain.arun(question)


class AVectorstoreIndexCreator(VectorstoreIndexCreator):
    """Async logic for creating indexes."""

    def from_documents(self, documents: List[Document]) -> AVectorStoreIndexWrapper:
        """Create a vectorstore index from documents."""
        sub_docs = self.text_splitter.split_documents(documents)
        vectorstore = self.vectorstore_cls.from_documents(
            sub_docs, self.embedding, **self.vectorstore_kwargs
        )
        return AVectorStoreIndexWrapper(vectorstore=vectorstore)
