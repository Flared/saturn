from saturn_engine.core import TopicMessage

def capitalize(word: str) -> TopicMessage:
    return TopicMessage(args={"word": word.title()})
