1. First of all you must use google trends from your web browser while you are logged in with your Google Account;
2. In order to track the actual HTTP GET made: (I am using Chromium) Go into "More Tools" -> "Developers Tools" -> "Network" tab.
3. Visit the Google Trend page and perform a search for a trend; it will trigger a lot of HTTP requests on the left sidebar of the "Network" tab;
4. Identify the GET request (in my case it was /trends/explore?q=topic&geo=US) and right-click on it and select Copy -> Copy as cURL;
5. Then go to this page (https://curlconverter.com) and paste the cURL script on the left side and copy the "headers" dictionary you can find inside the python script generated on the right side of the page;
6. Then go to your code and subclass the TrendReq class, so you can pass the custom header just copied:

```python
from pytrends.request import TrendReq as UTrendReq
GET_METHOD='get'

import requests

headers = {
...
}


class TrendReq(UTrendReq):
    def _get_data(self, url, method=GET_METHOD, trim_chars=0, **kwargs):
        return super()._get_data(url, method=GET_METHOD, trim_chars=trim_chars, headers=headers, **kwargs)
```

7. Remove any "import TrendReq" from your code since now it will use this you just created;
8. Retry again;
9. If in any future the error message comes back: repeat the procedure. You need to update the header dictionary with fresh values and it may trigger the captcha mechanism.
