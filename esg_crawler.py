import asyncio
import numpy as np
import aiosqlite
import time
import argparse
from loguru import logger
from playwright.async_api import async_playwright
from playwright.async_api import Browser, Page


async def get_companies(browser: Browser, year: str, market_type: str) -> list[dict]:
    page = await browser.new_page()
    page = await get_search_res(page, year, market_type)
    tables = page.locator('tbody')
    rows = tables.locator('tr')
    companies = []
    row_count = await rows.count()
    for i in range(row_count):
        cells = rows.nth(i)
        company_code = await cells.locator('td').nth(0).inner_text()
        company_name = await cells.locator('td').nth(1).inner_text()
        companies.append({'name': company_name, 'code': company_code})
    logger.info(f'Found {len(companies)} companies')
    return companies


async def get_search_res(page: Page, year: str, market_type: str) -> Page:
    await page.goto('https://esggenplus.twse.com.tw/inquiry/ghg-reduction')
    await page.wait_for_load_state('networkidle')
    await page.locator("body").click()
    await page.get_by_placeholder("市場別*").click()
    await page.get_by_text(market_type).click()
    await page.get_by_placeholder("報告年度*").click()
    await page.get_by_text(year).click()
    await page.get_by_role("button", name="查詢").click()
    await page.wait_for_load_state('networkidle')
    await page.inner_html('tbody')
    return page


async def get_report(page: Page, company_name: str):
    await page.get_by_role("row", name=f"{company_name} 詳細資料").locator("div").nth(1).click()
    modal_body_text = await page.locator('.modal-body').text_content()
    await page.get_by_label("Close").click()
    return modal_body_text


async def get_reports(
    page: Page, companies: list[dict], 
    year: str, market_type: str, db: aiosqlite.Connection):
    page = await get_search_res(page, year, market_type)
    res = []
    for company in companies:
        company['report'] = await get_report(page, company['name'])
        await db.execute("INSERT INTO company_reports (name, code, report, year, market_type) VALUES (?, ?, ?, ?, ?)",
                   (company['name'], company['code'], company['report'], year, market_type))
        await db.commit()
        logger.info(f'insert {company["name"]}, {company["code"]}, {company["report"][:15]}...')
        res.append(company)
    return res


async def main(num_tabs: int, year: str, market_type: str):
    db = await aiosqlite.connect('storage.sqlite3')
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS company_reports
            (id INTEGER PRIMARY KEY, name TEXT, code TEXT, report TEXT,
             year INTEGER, market_type TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
        """
    )
    await db.commit()
    p = await async_playwright().start()
    browser = await p.firefox.launch()
    companies = await get_companies(browser, year, market_type)
    pages = [await browser.new_page() for _ in range(num_tabs)]
    companies_chunks = np.array_split(companies, num_tabs)
    tasks = [get_reports(page, companies_chunk, year, market_type, db)
             for page, companies_chunk in zip(pages, companies_chunks)]
    res = await asyncio.gather(*tasks)
    await p.stop()
    return res


def get_parser():
    parser = argparse.ArgumentParser(description='ESG Crawler Script')
    parser.add_argument('--year', type=str, required=False,
                        default='2023', help='Report year')
    parser.add_argument('--num_tabs', type=int, required=False,
                        default=10, help='Number of browser tabs')
    parser.add_argument('--market_type', type=str, required=False,
                        default='上市', choices=['上市', '上櫃', '公發', '興櫃'], help='Market type')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    logger.info('Collecting data...')
    start = time.time()
    asyncio.run(main(args.num_tabs, args.year, args.market_type))
    elapsed_time = time.time() - start
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)
    logger.info(f'Time: {minutes}:{seconds:02d} minutes')
    logger.info('Finished collecting data.')
