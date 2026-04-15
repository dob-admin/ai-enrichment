// src/lib/claude.js
import Anthropic from '@anthropic-ai/sdk'
import { buildSystemPrompt, buildUserMessage } from '../prompts/enrichmentPrompt.js'

const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })

// Main enrichment call — returns parsed JSON result
export async function enrichRecord(record, sources) {
  const systemPrompt = buildSystemPrompt(record.airtableData.website)
  const userMessage = buildUserMessage(record, sources)

  try {
    const response = await client.messages.create({
      model: 'claude-sonnet-4-5',
      max_tokens: 2000,
      messages: [{ role: 'user', content: userMessage }],
      system: systemPrompt,
    })

    const text = response.content
      .filter(b => b.type === 'text')
      .map(b => b.text)
      .join('')

    // Strip any accidental markdown wrapping
    const clean = text.replace(/^```json\s*/i, '').replace(/\s*```$/, '').trim()
    return JSON.parse(clean)

  } catch (err) {
    if (err instanceof SyntaxError) {
      console.error('Claude returned invalid JSON:', err.message)
      return {
        confidence: 'low',
        missingFields: ['all'],
        validationIssues: ['Claude returned unparseable response'],
        error: true,
      }
    }
    throw err
  }
}

// Brand classification call — is this brand footwear or general merchandise?
export async function classifyBrand(brandName) {
  const response = await client.messages.create({
    model: 'claude-sonnet-4-5',
    max_tokens: 100,
    messages: [{
      role: 'user',
      content: `Is the brand "${brandName}" primarily a footwear brand or general merchandise?
Return JSON only: {"type": "footwear" | "general", "confidence": "high" | "medium" | "low"}`,
    }],
  })

  try {
    const text = response.content[0].text.trim()
    const clean = text.replace(/^```json\s*/i, '').replace(/\s*```$/, '').trim()
    return JSON.parse(clean)
  } catch {
    return { type: 'general', confidence: 'low' }
  }
}
