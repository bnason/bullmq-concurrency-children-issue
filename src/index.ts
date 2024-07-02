import { Queue, Worker } from 'bullmq'

console.log('BullMQ Concurrency Children Issue')

// Types
enum Step {
	Initial = 0,
	Waiting = 1,
	Finish = 2,
}

type ParentData = {
	step: Step
}

type ChildData = {
	foo?: string
}

// Redis Connection Info
const connection = {
	host: 'localhost',
	port: 6389,
}

// Parent Queue
const parentQueue = new Queue<ParentData>('parentQueue', { connection })
parentQueue.on('error', (error) => console.error('parent queue error', error))
parentQueue.on('ioredis:close', () => console.error('parent queue ioredis:close'))
parentQueue.on('cleaned', () => console.error('parent queue cleaned'))
parentQueue.on('paused', () => console.error('parent queue paused'))
parentQueue.on('resumed', () => console.error('parent queue resumed'))
parentQueue.on('progress', (job) => console.error('parent queue progress', job.id))
parentQueue.on('removed', (job) => console.error('parent queue removed', job.id))
parentQueue.on('waiting', (job) => console.error('parent queue waiting', job.id))
console.log('Created parent queue')

// Child Queue
const childQueue = new Queue<ChildData>('childQueue', { connection })
childQueue.on('error', (error) => console.error('child queue error', error))
console.log('Created child queue')

// Child Worker
const childWorker = new Worker<ChildData>(
	childQueue.name,
	async (job) => {
		console.log(`${job.name} - start child processor`)
		await new Promise<void>((resolve) => setTimeout(() => resolve(), 1000))
	},
	{
		connection,
		concurrency: 1,
	},
)
childWorker.on('error', (error) => console.error('child worker error', error))
console.log('Created child worker')

// Parent Worker
const parentWorker = new Worker<ParentData>(
	parentQueue.name,
	async (job, token) => {
		console.log(`${job.name} - start parent processor`)

		let step = job.data.step || Step.Initial
		while (step !== Step.Finish) {
			switch (step) {
				case Step.Initial: {
					console.log('Parent initializing step')

					const children: Parameters<typeof childQueue.addBulk>[0] = Array.from(Array(25)).map((v, i) => ({
						name: `${job.name}:child_job:${i}`,
						data: { foo: `bar-${i}` },
						opts: {
							parent: {
								id: job.id || '', // Why is id optional in job but not in parent's job id?
								queue: job.queueQualifiedName,
							},
						},
					}))

					await childQueue.addBulk(children)

					step = Step.Waiting
					await job.updateData({
						step,
					})

					break
				}
				case Step.Waiting: {
					console.log('Parent waiting step')

					const shouldWait = await job.moveToWaitingChildren(token || '') // Why processor's token is optional but moveToWaitingChildren is not?
					if (shouldWait) {
						console.log('Parent should wait')
						return
					}

					// We should reach this point only when all children have finished / failed
					console.log('Parent is done waiting, all children should be finished')
					step = Step.Finish
					await job.updateData({
						step,
					})

					return step
				}
				default: {
					throw new Error('invalid step')
				}
			}
		}
	},
	{ connection },
)
parentWorker.on('error', (error) => console.error('child worker error', error))
console.log('Created parent worker')

// Add a parent job
console.log('Adding parent jobs')
const parentJobs = [
	await parentQueue
		.add('parent_job:1', { step: Step.Initial })
		.catch((error) => console.error('Error adding parent job', error)),
	await parentQueue
		.add('parent_job:2', { step: Step.Initial })
		.catch((error) => console.error('Error adding parent job', error)),
]
console.log('Added parent jobs')

setInterval(async () => {
	for (const parentJob of parentJobs) {
		if (!parentJob) return

		const job = await parentQueue.getJob(parentJob.id || '')
		if (!job) return

		console.log(`Parent status=${await job.data.step} isWaitingChildren=${await job.isWaitingChildren()}`)
	}
}, 10000)
console.log('Created check interval')
