
import pytest
from bs4 import BeautifulSoup
from src.webscraper import soup_html, get_links_inside_table


def test_soup_html(requests_mock):
    # Mock the requests library to return a sample HTML response
    url = "http://example.com"
    html = "<html><body><h1>Alien invasion incoming!</h1></body></html>"
    requests_mock.get(url, text=html)

    # Call the function and assert the returned object is a BeautifulSoup instance
    result = soup_html(url)
    assert isinstance(result, BeautifulSoup)

    # Assert that the parsed HTML contains the expected content
    assert result.find("h1").text == "Alien invasion incoming!"


def test_get_links_inside_table(mocker):
    # Mock the soup_html function to return a sample BeautifulSoup object
    html = """
        <html>
            <body>
                <table>
                    <tr>
                        <td><a href="http://example.com/page1">Link 1</a></td>
                        <td><a href="http://example.com/page2">Link 2</a></td>
                    </tr>
                    <tr>
                        <td><a href="http://example.com/page3">Link 3</a></td>
                        <td><a href="http://example.com/page4">Link 4</a></td>
                    </tr>
                </table>
            </body>
        </html>
    """
    soup_mock = BeautifulSoup(html, "html.parser")
    mocker.patch("src.webscraper.soup_html", return_value=soup_mock)

    # Call the function and assert the returned value is a list
    result = get_links_inside_table("http://example.com")
    assert isinstance(result, list)

    # Assert that the returned list contains the expected links
    expected_links = [
        "http://example.com/page1",
        "http://example.com/page2",
        "http://example.com/page3",
        "http://example.com/page4",
    ]
    assert result == expected_links
