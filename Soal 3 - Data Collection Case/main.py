import asyncio
import httpx
import json
import polars as pl
from bs4 import BeautifulSoup
from pathlib import Path
from tqdm.asyncio import tqdm_asyncio
from typing import List, Dict, Tuple

BASE_URL = "https://www.fortiguard.com/encyclopedia"
MAX_PAGES = [7, 34, 179, 447, 286]  
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "https://www.fortiguard.com/"
}
TIMEOUT = 20
MAX_RETRIES = 5
CONCURRENCY_PER_LEVEL = 8  
DELAY_BETWEEN_REQUESTS = 0.5 

Path("datasets").mkdir(exist_ok=True)


# Async call for fetching page fortiguard 
async def fetch_page(client: httpx.AsyncClient, level: int, page: int) -> Tuple[int, int, str]:
    url = f"{BASE_URL}?type=ips&risk={level}&page={page}"
    for attempt in range(MAX_RETRIES):
        try:
            response = await client.get(url, headers=HEADERS, timeout=TIMEOUT)
            response.raise_for_status()

            if "<!DOCTYPE html>" in response.text[:15].strip():
                return (level, page, response.text)
            raise httpx.HTTPError("Invalid response format")
            
        except Exception as e:
            await asyncio.sleep(DELAY_BETWEEN_REQUESTS * (attempt + 1))
            if attempt == MAX_RETRIES - 1:
                return (level, page, None)


# for extracting title and url for every row in rank and page fortiguard
def parse_html(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    data = []
    
    for row in soup.select('div.row[onclick], tr[onclick]'):
        try:
            onclick = row.get('onclick', '')
            if "'" in onclick:
                href = onclick.split("'")[1]
            else:
                href = row.find('a')['href']
            
            title = None
            for selector in ['b', 'h4', 'td:nth-of-type(1)', 'a']:
                if elem := row.select_one(selector):
                    title = elem.get_text(strip=True)
                    if title:
                        break
            
            if href and title:
                data.append({
                    "title": title,
                    "link": f"https://www.fortiguard.com{href}"
                })
        except Exception:
            continue
    return data

# focused on getting and combining data from all rank 
async def process_level(level: int):
    async with httpx.AsyncClient() as client:
        total_pages = MAX_PAGES[level-1]
        tasks = [fetch_page(client, level, p) for p in range(1, total_pages + 1)]
        
        results = []
        skipped = []
        
        try:
            semaphore = asyncio.Semaphore(CONCURRENCY_PER_LEVEL)
            
            async def process_task(task):
                async with semaphore:
                    return await task
            
            for future in tqdm_asyncio.as_completed(
                [process_task(t) for t in tasks], 
                desc=f"Level {level}",
                total=total_pages
            ):
                lvl, page, html = await future
                if not html:
                    skipped.append(page)
                    continue
                
                if page_data := parse_html(html):
                    results.extend(page_data)
                else:
                    skipped.append(page)

        finally:
            if results:
                df = pl.DataFrame(results).unique()
                df.write_csv(f"datasets/forti_lists_{level}.csv")
            else:
                pl.DataFrame([]).write_csv(f"datasets/forti_lists_{level}.csv")
            
            return skipped


async def main():
    all_skipped = {}
    
    for level in range(1, 6):
        try:
            skipped = await process_level(level)
            if skipped:
                all_skipped[str(level)] = skipped
            print(f"Completed Level {level}")
        except Exception as e:
            print(f"Critical error in Level {level}: {str(e)}")
            all_skipped[str(level)] = list(range(1, MAX_PAGES[level-1]+1))
    
    if all_skipped:
        with open("datasets/skipped.json", "w") as f:
            json.dump(all_skipped, f, indent=2)

if __name__ == "__main__":
    asyncio.run(main())
