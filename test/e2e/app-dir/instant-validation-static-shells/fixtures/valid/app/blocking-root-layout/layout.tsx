import { connection } from 'next/server'

export const unstable_prefetch = false

export default async function RootLayout({ children }) {
  await connection()
  return (
    <html>
      <body>
        <p>
          This is a blocking root layout. It is configured with{' '}
          <code>unstable_prefetch = false</code>, so it should not be required
          to produce a static shell.
        </p>
        <hr />
        {children}
      </body>
    </html>
  )
}
