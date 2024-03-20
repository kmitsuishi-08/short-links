import fastify from "fastify";
import { z } from "zod"
import { sql } from "./lib/postgres";
import postgres from "postgres";
import { redis } from "./lib/redis";

const app = fastify() 

app.get("/:code", async (request, reply)=> {
    const getLinkSchema = z.object({
        code: z.string().min(3)
    })

    const { code } = getLinkSchema.parse(request.params)

    const result = await sql/*sql*/`
        SELECT original_url
        FROM short_links
        WHERE code = ${code}
    `
    if(result.length === 0)
        return reply.status(404).send({message: "Link not found!"})

    const link = result[0]

    await redis.zIncrBy('metrics', 1, code)

    return reply.redirect(301, link.original_url)
})

app.get("/api/links", async () => {
    const results = await sql/*sql*/`
        SELECT id, code, original_url, created_at
        FROM short_links
        ORDER BY created_at DESC
    `
    return results
})

app.post("/api/links", async (request, reply) => {
    const createLinkSchema = z.object({
        code: z.string().min(3),
        url: z.string().url()
    })

    const { code, url } = createLinkSchema.parse(request.body)

    try {

        const searchResult = await sql/*sql*/`
            SELECT id 
            FROM short_links 
            WHERE code = ${code}
        `
        if(searchResult.length !== 0)
            return reply.status(400).send({message: "This code is already being used"})

        const result = await sql/*sql*/`
            INSERT INTO short_links (code, original_url)
            VALUES(${code}, ${url})
            RETURNING id
        `
        const link = result[0]

        return reply.status(201).send({ shortLinkId: link.id })
    } catch (err) {
        if(err instanceof postgres.PostgresError){
            if(err.code === "23505"){
                return reply.status(400).send({message: "Duplicated code!"})
            }
        }

        console.log(err)
        return reply.status(500).send({message: "Internal error."})
    }
})

app.get("/api/metrics", async () => {
    const result = await redis.zRangeByScoreWithScores('metrics', 0, 50)

    const metrics = result
        .sort((a, b) => b.score - a.score)
        .map(item => {
            return {
                code: item.value,
                clicks: item.score
            }
        })
    
    return metrics
})

app.listen({
    port: 3333,
}).then(() => {
    console.log("HTTP server running!")
})