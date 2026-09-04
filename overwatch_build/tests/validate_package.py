import json, py_compile
from pathlib import Path
root=Path(__file__).parents[1]
for py in (root/'scripts').glob('*.py'):
    py_compile.compile(str(py),doraise=True)
workflows=list((root/'n8n').glob('*.json'))
assert len(workflows)==17, len(workflows)
for p in workflows: json.loads(p.read_text())
required=['OW-01_Scraper.json','OW-02_Scoring.json','OW-03_Content_Writer.json','OW-04_Fact_Checker.json','OW-05_Grouping.json','OW-06_Media.json','OW-07_Caption_Generator.json','OW-08_Scheduler_Publisher.json','OW-09_Watchtower.json','OW-10_Telegram_Bot.json','OW-11_Error_Handler.json','OW-12_Insights.json','OW-13_Human_Review.json','OW-14_Queue_Manager.json','OW-00_Master_Orchestrator_Smoke_Test.json','OW-DB_Init.json','connection-map.json']
assert sorted(p.name for p in workflows)==sorted(required)
print('PACKAGE_VALID',len(workflows))
