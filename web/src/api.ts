// /movie-tv-analytics/web/src/api.ts
import axios from 'axios'

// Always talk to the same origin as the browser.
// In dev, Vite proxies "/api" to the Flask server (see vite.config.ts).
// You can still override with VITE_API_URL if you need an absolute URL.
const envUrl = (import.meta as any).env?.VITE_API_URL as string | undefined
const baseURL = envUrl && envUrl.trim().length > 0 ? envUrl : '/api'
export const api = axios.create({ baseURL })

export type MediaItem = {
	id?: number
	tmdb_id: number
	media_type: 'movie' | 'tv'
	title: string
	overview?: string
	poster_path?: string
	backdrop_path?: string
	vote_average?: number
	vote_count?: number
	popularity?: number
	release_date?: string
	genres?: string[]
	original_language?: string
}

export type TrendingItem = {
	item_id: number
	tmdb_id: number
	media_type: 'movie' | 'show'
	title: string
	overview?: string
	poster_url?: string | null
	backdrop_url?: string | null
	tmdb_vote_avg?: number | null
	release_date?: string | null
	genres: string[]
}

export async function getSummary() {
	const { data } = await api.get('/summary')
	return data as {
		total_items: number
		movies: number
		tv: number
		avg_rating: number
		top_genres: { genre: string; count: number }[]
		languages: { language: string; count: number }[]
	}
}

export async function getList(type: 'movie' | 'tv', sort = 'popularity', page = 1, limit = 20) {
	const path = type === 'movie' ? '/movies' : '/tv'
	const { data } = await api.get(path, { params: { sort, page, limit } })
	return data as { total: number; page: number; results: MediaItem[] }
}

export async function refresh(pages = 1) {
	const { data } = await api.post('/refresh', null, { params: { pages } })
	return data as { ok: boolean; inserted: number; updated: number; total: number }
}

export async function search(q: string, page = 1) {
	const { data } = await api.get('/search', { params: { q, page } })
	return data as { page: number; results: MediaItem[]; total_results: number }
}

export type UserRow = { user: string; email: string; password: string }

export async function getUsers() {
	const { data } = await api.get('/users')
	return data as UserRow[]
}

export async function getHealth() {
	const { data } = await api.get('/health')
	return data as { status: string }
}

export async function getTrending(period: 'weekly' | 'monthly' | 'all' = 'weekly', limit = 20) {
	const { data } = await api.get('/trending', { params: { period, limit } })
	return (data?.results ?? []) as TrendingItem[]
}

export async function getNewReleases(limit = 12, type: 'all' | 'movie' | 'tv' = 'all') {
	const { data } = await api.get('/new-releases', { params: { limit, type } })
	return (data?.results ?? []) as MediaItem[]
}

export type LoginResponse = { ok: true; user: string; email: string } | { ok: false; error: string }

export async function login(email: string, password: string) {
	try {
		const { data } = await api.post('/login', { email, password })
		return data as LoginResponse
	} catch (err: any) {
		// Normalize axios errors into a consistent shape so UI can handle them
		const msg = (err?.response?.data?.error) || err?.message || 'Login request failed'
		return { ok: false, error: String(msg) } as LoginResponse
	}
}

export type SignupResponse = { ok: true; user: string; email: string } | { ok: false; error: string }

export async function signup(email: string, password: string, username?: string) {
	try {
		const { data } = await api.post('/signup', { email, password, username })
		return data as SignupResponse
	} catch (err: any) {
		const msg = (err?.response?.data?.error) || err?.message || 'Signup request failed'
		return { ok: false, error: String(msg) } as SignupResponse
	}
}
