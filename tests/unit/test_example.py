def test_example(testfile):
    """
    Example test that uses the custom `testfile` fixture from `conftest.py`.

    This reads a file for the __files subdirectory, intended to host test-only
    files. The test fails intentionally, to make sure it is edited or removed
    after generating a new project from this template.
    """
    f = testfile("example_test_file.txt")
    actual_file_content = f.read_text()
    assert actual_file_content == "ginger", "Unexpected file content!"
