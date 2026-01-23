export const unstable_prefetch = { mode: 'static' }

export default async function Layout({ children }) {
  return (
    <div>
      <p>
        This is a layout with{' '}
        <code>{`unstable_prefetch = { mode: 'static' }`}</code>.
      </p>
      <hr />
      {children}
    </div>
  )
}
