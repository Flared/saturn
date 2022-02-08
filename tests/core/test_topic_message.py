from saturn_engine.core.topic import TopicMessage
from saturn_engine.utils.options import asdict
from saturn_engine.utils.options import fromdict


def test_save_load_topic_message() -> None:
    m = TopicMessage(
        args={
            "hey": "ho",
            "none": None,
        }
    )
    m_loaded = fromdict(asdict(m), TopicMessage)
    assert m_loaded.args["hey"] == "ho"
    assert m_loaded.args["none"] is None
