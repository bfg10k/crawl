create table page(url text primary key, content text, err text);
create index page_empty_content_idx on page(content) where content is null;
