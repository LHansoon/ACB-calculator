# ACB-calculator

ACB calculator for ESPP stock purchase and sell



现在的状况比较麻烦，因为本质上就是在做数据清洗和统合

首先是WS

## WS

1. 首先你需要手动自己去WS的网页端，自己设置filter，然后运行：
```aiignore
\\ 这个是用来自动点击最下面的load more来load所有东西的
const interval = setInterval(() => {
  const btn = [...document.querySelectorAll('button')]
    .find(b => b.textContent.trim() === 'Load more');

  if (!btn) {
    console.log('❌ Load more 按钮没了，停止');
    clearInterval(interval);
    return;
  }

  btn.click();
  console.log('✅ Clicked Load more');
}, 500); // 500ms = 半秒
```
2. 第二步，你需要运行，注意你需要自己填入每一个entry的class信息。WS是用的框架自动生成的class name，所以每次都会变，但是好在我们需要点击的entry都共用一个class name
```aiignore
let buttons_all_list = document.querySelectorAll("button.sc-fc088cf7-0.fCbYLP.sc-ecac9ab9-0.icjbOy");
buttons_all_list.forEach((btn, i) => {
  setTimeout(() => {
    btn.click();
  }, i * 200); // 每 0.5 秒点一个
});
```
3. 等到上面的两个都运行结束，你就会发现在Network tab下面有了大量的GraphQL请求，这就是我们需要的东西。
4. filter输入GraphQL，然后导出他们的.har文件
5. 这个har文件包含了所有的graghQL的request和response：
   1. 内容物为大量的json格式数据
   2. ⚠️虽然我们的js script是一个一个往下按的，但是请求毕竟都是async的，所以其实并不保证request和response的顺序，有需要的话可以降低一点点击频率。
6. 去Wealthsimple文件夹里面call har_file_process，就把文件丢进去就可以了
    1. 首先处理下载的har file