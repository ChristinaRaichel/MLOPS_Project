import hello

def test_prepare_text():
    text = "hiope"
    actual = hello.print_output(text)
    expected = "Hello World"

    assert actual == expected



