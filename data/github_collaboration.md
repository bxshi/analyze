# Github Collaboration

## Extract data from Google Bigquery

```sql
SELECT real_user as user, repo_name as repo FROM (
  SELECT actor_login, repo_name, 
	      JSON_EXTRACT(payload, '$.action') as action, 
				JSON_EXTRACT(payload, "$.pull_request.merged") as merged,
				SPLIT(JSON_EXTRACT(payload, "$.pull_request.user.login"), '"') as real_user,
			FROM [githubarchive:month.201502] 
			where type="PullRequestEvent"
) WHERE merged = "true" 
``` 

```sql
SELECT actor_login as user, repo_name as repo
	FROM [githubarchive:month.201501] 
	WHERE type="PushEvent"
```

## Combine tables above, deduplicate and download to a csv file

## Use `./tools/` to preprocess
