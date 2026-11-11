// Requires an installed Playwright runtime: NODE_PATH=<node_modules> node browser_test.cjs <url>
const assert=require('node:assert/strict');
const {chromium}=require('playwright');
(async()=>{
 const browser=await chromium.launch({headless:true});
 try{
 const page=await browser.newPage({viewport:{width:1440,height:1100}});
 const errors=[];page.on('pageerror',error=>errors.push(error.message));
 let document={choices:{3:{choice:'all',bytes:123,custom:'preserve unchanged'},999999:{choice:'none',custom:'absent stream'}},custom:'document metadata'};
 let posted;
 await page.route('**/api/selection',route=>{
  if(route.request().method()==='POST'){posted=route.request().postDataJSON();document={...posted,saved_at:'2026-11-11T20:00:00Z',saved_by:'deniz@aydemir.us'};return route.fulfill({json:{saved_at:document.saved_at}});}
  return route.fulfill({json:document});
 });
 await page.goto(process.argv[2]);await page.locator('.stream').first().waitFor();
 const data=await page.evaluate(()=>fetch('coverage.json').then(r=>r.json()));
 assert.equal(await page.locator('.stream').count(),data.streams.length);
 assert.equal(await page.locator('.day').count(),data.streams.reduce((n,r)=>n+r.seconds.length,0));
 assert.deepEqual(await page.evaluate(()=>[0,60479,60480,77760,77761].map(grade)),['zero','red','yellow','yellow','green']);
 for(const key of ['total_hours','avg_coverage','days90','days_recorded','all']){
  await page.selectOption('#sort',key);
  const expected=[...data.streams].sort((a,b)=>(key==='all'?(b.r2_raw_keep+b.r2_raw_out+b.r2_st_keep+b.r2_st_out)-(a.r2_raw_keep+a.r2_raw_out+a.r2_st_keep+a.r2_st_out):b[key]-a[key])||a.stream_id-b.stream_id)[0].stream_id;
  assert.equal(Number(await page.locator('.stream').first().getAttribute('data-stream')),expected);
 }
 await page.fill('#q','Tokyo');assert.ok(await page.locator('.stream').count()>0);
 await page.fill('#q','no-such-stream-xyz');assert.equal(await page.locator('.stream').count(),0);
 await page.fill('#q','78');
 await page.locator('[data-stream="78"] input[value="keep"]').check();
 await page.locator('#save').click();await page.waitForFunction(()=>document.getElementById('st').textContent.startsWith('Saved '));
 assert.deepEqual(posted.choices['3'],{choice:'all',bytes:123,custom:'preserve unchanged'});
 assert.deepEqual(posted.choices['999999'],{choice:'none',custom:'absent stream'});
 assert.equal(posted.custom,'document metadata');assert.equal(posted.choices['78'].choice,'keep');
 await page.fill('#q','');await page.selectOption('#filter','decided');
 assert.equal(await page.locator('.stream').count(),2);
 await page.selectOption('#bulk','none');assert.equal(await page.locator('.opts input:checked').count(),2);
 await page.selectOption('#filter','gaps');
 assert.equal(await page.locator('.stream').count(),data.streams.filter(r=>r.seconds.includes(0)).length);
 await page.selectOption('#filter','all');await page.selectOption('#sort','name');
 await page.locator('.day').first().click();assert.match(await page.locator('.day-status').first().textContent(),/coverage/);
 await page.locator('.thumb').first().click();assert.equal(await page.locator('dialog[open]').count(),1);await page.keyboard.press('Escape');
 for(const width of [390,768,1440]){
  await page.setViewportSize({width,height:1000});
  assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'horizontal overflow at '+width);
 }
 await page.emulateMedia({colorScheme:'dark'});
 assert.equal(await page.evaluate(()=>getComputedStyle(document.documentElement).getPropertyValue('--bg').trim()),'#141413');
 assert.deepEqual(errors,[]);
 const failure=await browser.newPage();await failure.route('**/api/selection',route=>route.fulfill({status:500,body:'failed'}));
 await failure.goto(process.argv[2]);await failure.waitForFunction(()=>document.getElementById('st').textContent.includes('Reload to retry'));
 assert.equal(await failure.locator('#save').isDisabled(),true);
 console.log('Browser: all day cells, thresholds, sorting, search, filters, bulk picks, preserving save round-trip, tap, thumbnails, 390/768/1440px, dark mode and load failure passed');
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
